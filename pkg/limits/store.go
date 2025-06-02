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

	"github.com/grafana/loki/v3/pkg/limits/proto"
)

// The number of stripe locks.
const numStripes = 64

var (
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
	activeWindow  time.Duration
	rateWindow    time.Duration
	bucketSize    time.Duration
	numBuckets    int
	numPartitions int
	stripes       []map[string]tenantStats
	locks         []stripeLock

	// Used for tests.
	clock quartz.Clock
}

// A tenantStats contains per-partition stream statistics for a tenant.
type tenantStats map[int32]map[uint64]streamStats

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

// A tenantStatsEntry lets the owner of an exclusive lock on the stripe
// read and write the stats for a tenant. It is inspried by the Entry enum
// in Rust's std::collections::hash_map.
type tenantStatsEntry struct {
	stats              tenantStats
	getPartitionForHash   func(uint64) int32
	withinActiveWindow func(int64) bool
	withinRateWindow   func(int64) bool
	clock              quartz.Clock
}

// All returna an iterator over all active streams.
func (e tenantStatsEntry) All() iter.Seq2[int32, streamStats] {
	return func(yield func(int32, streamStats) bool) {
		for partition, streams := range e.stats {
			for _, stream := range streams {
				if e.withinActiveWindow(stream.lastSeenAt) {
					stream.rateBuckets = getActiveRateBuckets(
						stream.rateBuckets,
						e.withinRateWindow,
					)
					yield(partition, stream)
				}
			}
		}
	}
}

// CountStreams counts the total number of streams, including both active
// and expired streams.
func (e tenantStatsEntry) CountStreams() int {
	var n int
	for _, streams := range e.stats {
		n += len(streams)
	}
	return n
}

// CountPartitionStreams counts the total number of streams for the partition,
// including both active and expired streams.
func (e tenantStatsEntry) CountPartitionStreams(partition int32) int {
	return len(e.stats[partition])
}

// Get returns the stats for the stream. It returns false if the stream has
// expired or does not exist.
func (e tenantStatsEntry) Get(streamHash uint64) (streamStats, bool) {
	partition := e.getPartitionForHash(streamHash)
	streams, ok := e.stats[partition]
	if !ok {
		return streamStats{}, false
	}
	stats, ok := streams[streamHash]
	return stats, ok
}

// 
func (e tenantStatsEntry) Update(metadata proto.StreamMetadata, seenAt time.Time) error {
	if !e.withinActiveWindow(seenAt.UnixNano()) {
		return errOutsideActiveWindow
	}
	partition := s.getPartitionForHash(metadata.StreamHash)
	streamHash, totalSize := metadata.StreamHash, metadata.TotalSize
	// Get the stats for the stream.
	stream, ok := s.stripes[i][tenant][partition][streamHash]
	cutoff := seenAt.Add(-s.activeWindow).UnixNano()
	// If the stream does not exist, or it has expired, reset it.
	if !ok || stream.lastSeenAt < cutoff {
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
	s.stripes[i][tenant][partition][streamHash] = stream
	return nil
}

// newStatsStore returns a new statsStore.
func newStatsStore(activeWindow, rateWindow, bucketSize time.Duration, numPartitions int, reg prometheus.Registerer) (*statsStore, error) {
	s := &statsStore{
		activeWindow:  activeWindow,
		rateWindow:    rateWindow,
		bucketSize:    bucketSize,
		numBuckets:    int(rateWindow / bucketSize),
		numPartitions: numPartitions,
		stripes:       make([]map[string]tenantStats, numStripes),
		locks:         make([]stripeLock, numStripes),
		clock:         quartz.NewReal(),
	}
	for i := range s.stripes {
		s.stripes[i] = make(map[string]tenantStats)
	}
	if err := reg.Register(s); err != nil {
		return nil, fmt.Errorf("failed to register metrics: %w", err)
	}
	return s, nil
}

// All returns an iterator over the statistics for all active streams. As
// this method acquires a read lock, the iterator must not block.
func (s *statsStore) All() iter.Seq2[string, streamStats] {
	// To prevent time moving forward while iterating, use the current time
	// to check the active and rate window.
	var (
		now                = s.clock.Now()
		withinActiveWindow = s.newActiveWindowFunc(now)
		withinRateWindow   = s.newRateWindowFunc(now)
	)
	return func(yield func(string, streamStats) bool) {
		s.forEachRLock(func(i int) {
			for tenant, partitions := range s.stripes[i] {
				for _, streams := range partitions {
					for _, stream := range streams {
						if withinActiveWindow(stream.lastSeenAt) {
							stream.rateBuckets = getActiveRateBuckets(
								stream.rateBuckets,
								withinRateWindow,
							)
							yield(tenant, stream)
						}
					}
				}
			}
		})
	}
}

// AllForTenant returns an iterator over all active streams for the tenant.
// As this method acquires a read lock, the iterator must not block.
func (s *statsStore) AllForTenant(tenant string) iter.Seq[streamStats] {
	// To prevent time moving forward while iterating, use the current time
	// to check the active and rate window.
	var (
		now                = s.clock.Now()
		withinActiveWindow = s.newActiveWindowFunc(now)
		withinRateWindow   = s.newRateWindowFunc(now)
	)
	return func(yield func(streamStats) bool) {
		s.withRLock(tenant, func(i int) {
			for _, streams := range s.stripes[i][tenant] {
				for _, stream := range streams {
					if withinActiveWindow(stream.lastSeenAt) {
						stream.rateBuckets = getActiveRateBuckets(
							stream.rateBuckets,
							withinRateWindow,
						)
						yield(stream)
					}
				}
			}
		})
	}
}

// Get returns the stats for the stream. It returns false if the stream has
// expired or does not exist.
func (s *statsStore) Get(tenant string, streamHash uint64) (streamStats, bool) {
	var (
		now                = s.clock.Now()
		withinActiveWindow = s.newActiveWindowFunc(now)
		withinRateWindow   = s.newRateWindowFunc(now)
		partition          = s.getPartitionForHash(streamHash)
		stats              streamStats
		ok                 bool
	)
	s.withRLock(tenant, func(i int) {
		stats, ok = s.get(i, tenant, partition, streamHash)
		if !ok {
			return
		}
		if !withinActiveWindow(stats.lastSeenAt) {
			ok = false
			return
		}
		stats.rateBuckets = getActiveRateBuckets(stats.rateBuckets, withinRateWindow)
	})
	return stats, ok
}

func (s *statsStore) ForTenant(tenant string, f func(entry tenantStatsEntry)) {
	now := s.clock.Now()
	s.withLock(tenant, func(i int) {
		f(tenantStatsEntry{
			stats:              s.stripes[i][tenant],
			getPartitionForHash:   s.getPartitionForHash,
			withinActiveWindow: s.newActiveWindowFunc(now),
			withinRateWindow:   s.newRateWindowFunc(now),
			clock:              s.clock,
		})
	})
}

func (s *statsStore) Update(tenant string, metadata *proto.StreamMetadata, seenAt time.Time) error {
	if !s.withinActiveWindow(seenAt.UnixNano()) {
		return errOutsideActiveWindow
	}
	partition := s.getPartitionForHash(metadata.StreamHash)
	s.withLock(tenant, func(i int) {
		s.update(i, tenant, partition, metadata, seenAt)
	})
	return nil
}

// Evict evicts all streams that have not been seen within the window.
func (s *statsStore) Evict() map[string]int {
	cutoff := s.clock.Now().Add(-s.activeWindow).UnixNano()
	evicted := make(map[string]int)
	s.forEachLock(func(i int) {
		for tenant, partitions := range s.stripes[i] {
			for partition, streams := range partitions {
				for streamHash, stream := range streams {
					if stream.lastSeenAt < cutoff {
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
		cutoff = s.clock.Now().Add(-s.activeWindow).UnixNano()
		active = make(map[string]int)
		total  = make(map[string]int)
	)
	// Count both the total number of active streams and the total number of
	// streams for each tenants.
	s.forEachRLock(func(i int) {
		for tenant, partitions := range s.stripes[i] {
			for _, streams := range partitions {
				for _, stream := range streams {
					total[tenant]++
					if stream.lastSeenAt >= cutoff {
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

func (s *statsStore) update(i int, tenant string, partition int32, metadata *proto.StreamMetadata, seenAt time.Time) {
	s.checkInitMap(i, tenant, partition)
	streamHash, totalSize := metadata.StreamHash, metadata.TotalSize
	// Get the stats for the stream.
	stream, ok := s.stripes[i][tenant][partition][streamHash]
	cutoff := seenAt.Add(-s.activeWindow).UnixNano()
	// If the stream does not exist, or it has expired, reset it.
	if !ok || stream.lastSeenAt < cutoff {
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
	s.stripes[i][tenant][partition][streamHash] = stream
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

// getPartitionForHash returns the partition for the hash.
func (s *statsStore) getPartitionForHash(hash uint64) int32 {
	return int32(hash % uint64(s.numPartitions))
}

// withinActiveWindow returns true if t is within the active window.
func (s *statsStore) withinActiveWindow(t int64) bool {
	return s.clock.Now().Add(-s.activeWindow).UnixNano() <= t
}

// newActiveWindowFunc returns a func that returns true if t is within
// the active window. It memoizes the start of the active time window.
func (s *statsStore) newActiveWindowFunc(now time.Time) func(t int64) bool {
	activeWindowStart := now.Add(-s.activeWindow).UnixNano()
	return func(t int64) bool {
		return activeWindowStart <= t
	}
}

// withinRateWindow returns true if t is within the rate window.
func (s *statsStore) withinRateWindow(t int64) bool {
	return s.clock.Now().Add(-s.rateWindow).UnixNano() <= t
}

// newRateWindowFunc returns a func that returns true if t is within
// the rate window. It memoizes the start of the rate time window.
func (s *statsStore) newRateWindowFunc(now time.Time) func(t int64) bool {
	rateWindowStart := now.Add(-s.rateWindow).UnixNano()
	return func(t int64) bool {
		return rateWindowStart <= t
	}
}

// checkInitMap checks if the maps for the tenant and partition are
// initialized, and if not, initializes them. It must not be called without
// the stripe lock for i.
func (s *statsStore) checkInitMap(i int, tenant string, partition int32) {
	if _, ok := s.stripes[i][tenant]; !ok {
		s.stripes[i][tenant] = make(tenantStats)
	}
	if _, ok := s.stripes[i][tenant][partition]; !ok {
		s.stripes[i][tenant][partition] = make(map[uint64]streamStats)
	}
}

// Used in tests. Is not goroutine-safe.
func (s *statsStore) getForTests(tenant string, streamHash uint64) (streamStats, bool) {
	partition := s.getPartitionForHash(streamHash)
	i := s.getStripe(tenant)
	return s.get(i, tenant, partition, streamHash)
}

// Used in tests. Is not goroutine-safe.
func (s *statsStore) setForTests(tenant string, stream streamStats) {
	partition := s.getPartitionForHash(stream.hash)
	s.withLock(tenant, func(i int) {
		s.checkInitMap(i, tenant, partition)
		s.stripes[i][tenant][partition][stream.hash] = stream
	})
}

// getActiveRateBuckets returns the buckets within the active window.
func getActiveRateBuckets(buckets []rateBucket, withinRateWindow func(int64) bool) []rateBucket {
	result := make([]rateBucket, 0, len(buckets))
	for _, bucket := range buckets {
		if withinRateWindow(bucket.timestamp) {
			result = append(result, bucket)
		}
	}
	return result
}
