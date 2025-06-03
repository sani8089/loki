package limits

import (
	"errors"
	"fmt"
	"hash/fnv"
	"iter"
	"sync"
	"time"

	"github.com/coder/quartz"
	"github.com/prometheus/client_golang/prometheus"
)

// The number of stripe locks.
const numStripes = 64

var (
	// errOutsideActiveWindow is returned when the data is too old to be
	// included in the stream statistics.
	errOutsideActiveWindow = errors.New("outside active time window")
)

var (
	tenantStreamsDesc = prometheus.NewDesc(
		"loki_ingest_limits_streams",
		"The current number of streams per tenant, including streams outside the active window.",
		[]string{"tenant"},
		nil,
	)
	tenantActiveStreamsDesc = prometheus.NewDesc(
		"loki_ingest_limits_active_streams",
		"The current number of active streams per tenant.",
		[]string{"tenant"},
		nil,
	)
)

// A statsStore stores per-tenant stream statistics.
type statsStore struct {
	activeWindow     time.Duration
	rateWindow       time.Duration
	bucketSize       time.Duration
	numBuckets       int
	numPartitions    int
	getPartitionFunc getPartitionFunc
	stripes          []map[string]tenantStats
	locks            []stripeLock

	// Used for tests.
	clock quartz.Clock
}

// A tenantStats contains the per-partition stream statistics for a tenant.
type tenantStats map[int32]partitionStats

// A partitionStats contains the stream statistics for a partition.
type partitionStats map[uint64]streamStats

// A streamStats contains the statistics for a stream.
type streamStats struct {
	hash        uint64
	lastSeenAt  int64
	totalSize   uint64
	rateBuckets []rateBucket
}

// A rateBucket contains the data rate for a fixed interval bucket in the rate
// window.
type rateBucket struct {
	timestamp int64
	size      uint64
}

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

// newStatsStore returns a new statsStore.
func newStatsStore(activeWindow, rateWindow, bucketSize time.Duration, numPartitions int, reg prometheus.Registerer) (*statsStore, error) {
	s := &statsStore{
		activeWindow:     activeWindow,
		rateWindow:       rateWindow,
		bucketSize:       bucketSize,
		numBuckets:       int(rateWindow / bucketSize),
		getPartitionFunc: newGetPartitionFunc(numPartitions),
		stripes:          make([]map[string]tenantStats, numStripes),
		locks:            make([]stripeLock, numStripes),
		clock:            quartz.NewReal(),
	}
	for i := range s.stripes {
		s.stripes[i] = make(map[string]tenantStats)
	}
	if err := reg.Register(s); err != nil {
		return nil, fmt.Errorf("failed to register metrics: %w", err)
	}
	return s, nil
}

// ActiveStreams returns an iterator over the statistics for all active
// streams. As this method acquires a read lock, the iterator must not block.
func (s *statsStore) ActiveStreams() iter.Seq2[string, streamStats] {
	// To prevent time moving forward while iterating, use the current time
	// to check the active and rate window.
	var (
		now            = s.clock.Now()
		inActiveWindow = newActiveWindowFunc(s.activeWindow, now)
		inRateWindow   = newRateWindowFunc(s.rateWindow, now)
	)
	return func(yield func(string, streamStats) bool) {
		s.forEachRLock(func(i int) {
			for tenant, partitions := range s.stripes[i] {
				for _, streams := range partitions {
					for _, stream := range streams {
						if inActiveWindow(stream.lastSeenAt) {
							stream.rateBuckets = rateBucketsInWindow(
								stream.rateBuckets,
								inRateWindow,
							)
							if !yield(tenant, stream) {
								return
							}
						}
					}
				}
			}
		})
	}
}

// WithTenant calls f with an exclusive lock. It can be used to read and
// write stream usage statistics for the tenant.
func (s *statsStore) WithTenant(tenant string, f func(*tenantStatsStore)) {
	now := s.clock.Now()
	s.withLock(tenant, func(i int) {
		if s.stripes[i][tenant] == nil {
			s.stripes[i][tenant] = make(tenantStats)
		}
		f(&tenantStatsStore{
			tenantStatsReadStore: tenantStatsReadStore{
				stats:          s.stripes[i][tenant],
				getPartitionFunc: s.getPartitionFunc,
				inActiveWindow: newActiveWindowFunc(s.activeWindow, now),
				inRateWindow:   newRateWindowFunc(s.rateWindow, now),
			},
			numBuckets: s.numBuckets,
			bucketSize: s.bucketSize,
		})
	})
}

// WithTenantRead calls f with a shared lock. It can be used to read stream
// usage statistics for the tenant.
func (s *statsStore) WithTenantRead(tenant string, f func(*tenantStatsReadStore)) {
	now := s.clock.Now()
	s.withRLock(tenant, func(i int) {
		f(&tenantStatsReadStore{
			stats:          s.stripes[i][tenant],
			getPartitionFunc: s.getPartitionFunc,
			inActiveWindow: newActiveWindowFunc(s.activeWindow, now),
			inRateWindow:   newRateWindowFunc(s.rateWindow, now),
		})
	})
}

// Update the statistics for the stream.
func (s *statsStore) Update(tenant string, streamHash, totalSize uint64, seenAt time.Time) error {
	var err error
	s.WithTenant(tenant, func(ts *tenantStatsStore) {
		err = ts.Update(streamHash, totalSize, seenAt)
	})
	return err
}

// Evict evicts all streams that have not been seen within the window.
func (s *statsStore) Evict() map[string]int {
	var (
		now            = s.clock.Now()
		inActiveWindow = newActiveWindowFunc(s.activeWindow, now)
		evicted        = make(map[string]int)
	)
	s.forEachLock(func(i int) {
		for tenant, partitions := range s.stripes[i] {
			for partition, streams := range partitions {
				for streamHash, stream := range streams {
					if !inActiveWindow(stream.lastSeenAt) {
						delete(s.stripes[i][tenant][partition], streamHash)
						evicted[tenant]++
					}
				}
			}
		}
	})
	return evicted
}

// EvictPartitions evicts all streams for the specified partitions.
func (s *statsStore) EvictPartitions(partitionsToEvict []int32) {
	s.forEachLock(func(i int) {
		for tenant, partitions := range s.stripes[i] {
			for _, partitionToEvict := range partitionsToEvict {
				delete(partitions, partitionToEvict)
			}
			if len(partitions) == 0 {
				delete(s.stripes[i], tenant)
			}
		}
	})
}

// Describe implements [prometheus.Collector].
func (s *statsStore) Describe(descs chan<- *prometheus.Desc) {
	descs <- tenantStreamsDesc
	descs <- tenantActiveStreamsDesc
}

// Collect implements [prometheus.Collector].
func (s *statsStore) Collect(metrics chan<- prometheus.Metric) {
	var (
		inActiveWindow = newActiveWindowFunc(s.activeWindow, s.clock.Now())
		active         = make(map[string]int)
		total          = make(map[string]int)
	)
	// Count both the total number of active streams and the total number of
	// streams for each tenants.
	s.forEachRLock(func(i int) {
		for tenant, partitions := range s.stripes[i] {
			for _, streams := range partitions {
				for _, stream := range streams {
					total[tenant]++
					if inActiveWindow(stream.lastSeenAt) {
						active[tenant]++
					}
				}
			}
		}
	})
	for tenant, numActiveStreams := range active {
		metrics <- prometheus.MustNewConstMetric(
			tenantActiveStreamsDesc,
			prometheus.GaugeValue,
			float64(numActiveStreams),
			tenant,
		)
	}
	for tenant, numStreams := range total {
		metrics <- prometheus.MustNewConstMetric(
			tenantStreamsDesc,
			prometheus.GaugeValue,
			float64(numStreams),
			tenant,
		)
	}
}

// forEachRLock executes fn with a shared lock for each stripe.
func (s *statsStore) forEachRLock(fn func(i int)) {
	for i := range s.stripes {
		s.locks[i].RLock()
		fn(i)
		s.locks[i].RUnlock()
	}
}

// forEachLock executes fn with an exclusive lock for each stripe.
func (s *statsStore) forEachLock(fn func(i int)) {
	for i := range s.stripes {
		s.locks[i].Lock()
		fn(i)
		s.locks[i].Unlock()
	}
}

// withRLock executes fn with a shared lock on the stripe.
func (s *statsStore) withRLock(tenant string, fn func(i int)) {
	i := s.getStripe(tenant)
	s.locks[i].RLock()
	defer s.locks[i].RUnlock()
	fn(i)
}

// withLock executes fn with an exclusive lock on the stripe.
func (s *statsStore) withLock(tenant string, fn func(i int)) {
	i := s.getStripe(tenant)
	s.locks[i].Lock()
	defer s.locks[i].Unlock()
	fn(i)
}

// getStripe returns the stripe index for the tenant.
func (s *statsStore) getStripe(tenant string) int {
	h := fnv.New32()
	_, _ = h.Write([]byte(tenant))
	return int(h.Sum32() % uint32(len(s.locks)))
}

// Used in tests. Is not goroutine-safe.
func (s *statsStore) getForTests(tenant string, streamHash uint64) (streamStats, bool) {
	partition := s.getPartitionFunc(streamHash)
	i := s.getStripe(tenant)
	return s.get(i, tenant, partition, streamHash)
}

func (s *statsStore) get(i int, tenant string, partition int32, streamHash uint64) (stream streamStats, ok bool) {
	partitions, ok := s.stripes[i][tenant]
	if !ok {
		return
	}
	streams, ok := partitions[partition]
	if !ok {
		return
	}
	stream, ok = streams[streamHash]
	return
}

// Used in tests.
func (s *statsStore) setForTests(tenant string, stream streamStats) {
	partition := s.getPartitionFunc(stream.hash)
	s.WithTenant(tenant, func(ts *tenantStatsStore) {
		if _, ok := ts.stats[partition]; !ok {
			ts.stats[partition] = make(map[uint64]streamStats)
		}
		ts.stats[partition][stream.hash] = stream
	})
}

// tenantStatsReadStore holds a shared reference to a tenant's stream
// statistics. It can be used to read stream usage statistics for the tenant.
// Since multiple goroutines can share the reference, it cannot be used for
// writes.
type tenantStatsReadStore struct {
	stats            tenantStats
	getPartitionFunc getPartitionFunc
	inActiveWindow   func(int64) bool
	inRateWindow     func(int64) bool
}

// ActiveStreams returns an iterator over all active streams.
func (s *tenantStatsReadStore) ActiveStreams() iter.Seq2[int32, streamStats] {
	return func(yield func(int32, streamStats) bool) {
		for partition, streams := range s.stats {
			for _, stream := range streams {
				if s.inActiveWindow(stream.lastSeenAt) {
					stream.rateBuckets = rateBucketsInWindow(
						stream.rateBuckets,
						s.inRateWindow,
					)
					if !yield(partition, stream) {
						return
					}
				}
			}
		}
	}
}

// CountStreams returns the total number of streams, including both active
// and expired streams, for all partitions.
func (s *tenantStatsReadStore) CountStreams() int {
	var n int
	for _, streams := range s.stats {
		n += len(streams)
	}
	return n
}

// CountPartitionStreams returns the total number of streams, including both
// active and expired streams, for the partition.
func (s *tenantStatsReadStore) CountPartitionStreams(partition int32) int {
	return len(s.stats[partition])
}

// Get returns the stats for the stream. It returns false if the stream has
// expired or does not exist.
func (s *tenantStatsReadStore) Get(streamHash uint64) (streamStats, bool) {
	partition := s.getPartitionFunc(streamHash)
	streams, ok := s.stats[partition]
	if !ok {
		return streamStats{}, false
	}
	stats, ok := streams[streamHash]
	return stats, ok
}

func (l *tenantStatsReadStore) Has(streamHash uint64) bool {
	return false
}

func (l *tenantStatsReadStore) IsExpired(streamHash uint64) bool {
	return false
}

// tenantStatsStore holds an exclusive reference to a tenant's stream
// statistics. It can be used to read and write stream usage statistics for
// the tenant.
type tenantStatsStore struct {
	tenantStatsReadStore
	numBuckets int
	bucketSize time.Duration
}

func (s *tenantStatsStore) Update(streamHash, totalSize uint64, seenAt time.Time) error {
	if !s.inActiveWindow(seenAt.UnixNano()) {
		return errOutsideActiveWindow
	}
	partition := s.getPartitionFunc(streamHash)
	if _, ok := s.stats[partition]; !ok {
		s.stats[partition] = make(map[uint64]streamStats)
	}
	// Get the stats for the stream.
	stream, ok := s.stats[partition][streamHash]
	// If the stream does not exist, or it has expired, reset it.
	if !ok || !s.inActiveWindow(stream.lastSeenAt) {
		stream.hash = streamHash
		stream.totalSize = 0
		stream.rateBuckets = make([]rateBucket, s.numBuckets)
	}
	seenAtUnixNano := seenAt.UnixNano()
	if stream.lastSeenAt <= seenAtUnixNano {
		stream.lastSeenAt = seenAtUnixNano
	}
	stream.totalSize += totalSize
	// rate buckets are implemented as a circular list. To update a rate
	// bucket we must first calculate the bucket index.
	bucketNum := seenAtUnixNano / int64(s.bucketSize)
	bucketIdx := int(bucketNum % int64(s.numBuckets))
	bucket := stream.rateBuckets[bucketIdx]
	// Once we have found the bucket, we then need to check if it is an old
	// bucket outside the rate window. If it is, we must reset it before we
	// can re-use it.
	bucketStart := seenAt.Truncate(s.bucketSize).UnixNano()
	if bucket.timestamp < bucketStart {
		bucket.timestamp = bucketStart
		bucket.size = 0
	}
	bucket.size += totalSize
	stream.rateBuckets[bucketIdx] = bucket
	s.stats[partition][streamHash] = stream
	return nil
}

type getPartitionFunc func(uint64) int32

func newGetPartitionFunc(numPartitions int) getPartitionFunc {
	return func(streamHash uint64) int32 {
		return int32(streamHash % uint64(numPartitions))
	}
}

// rateBucketsInWindow returns the buckets within the rate window.
func rateBucketsInWindow(buckets []rateBucket, withinRateWindow func(int64) bool) []rateBucket {
	result := make([]rateBucket, 0, len(buckets))
	for _, bucket := range buckets {
		if withinRateWindow(bucket.timestamp) {
			result = append(result, bucket)
		}
	}
	return result
}

// newActiveWindowFunc returns a func that returns true if t is within
// the active window. It memoizes the start of the active time window.
func newActiveWindowFunc(activeWindow time.Duration, now time.Time) func(t int64) bool {
	activeWindowStart := now.Add(-activeWindow).UnixNano()
	return func(t int64) bool {
		return activeWindowStart <= t
	}
}

// inActiveWindow returns true if t is within the active window.
func inActiveWindow(clock quartz.Clock, activeWindow time.Duration, t int64) bool {
	return clock.Now().Add(-activeWindow).UnixNano() <= t
}

// newRateWindowFunc returns a func that returns true if t is within
// the rate window. It memoizes the start of the rate time window.
func newRateWindowFunc(rateWindow time.Duration, now time.Time) func(t int64) bool {
	rateWindowStart := now.Add(-rateWindow).UnixNano()
	return func(t int64) bool {
		return rateWindowStart <= t
	}
}

// inRateWindow returns true if t is within the rate window.
func inRateWindow(clock quartz.Clock, rateWindow time.Duration, t int64) bool {
	return clock.Now().Add(-rateWindow).UnixNano() <= t
}
