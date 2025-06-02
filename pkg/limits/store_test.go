package limits

import (
	"fmt"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/limits/proto"
)

func TestStatsStore_All(t *testing.T) {
	t.Run("iterates all streams", func(t *testing.T) {
		s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 10, prometheus.NewRegistry())
		require.NoError(t, err)
		clock := quartz.NewMock(t)
		s.clock = clock
		for i := 0; i < 10; i++ {
			// Create 10 streams, one stream for each of the 10 partitions.
			require.NoError(t, s.Update(
				fmt.Sprintf("tenant%d", i),
				&proto.StreamMetadata{
					StreamHash: uint64(i),
					TotalSize:  100,
				},
				clock.Now()),
			)
		}
		// Assert that we can iterate all stored streams.
		expected := []uint64{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}
		actual := make([]uint64, 0, len(expected))
		for _, stream := range s.All() {
			actual = append(actual, stream.hash)
		}
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("does not iterate expired streams", func(t *testing.T) {
		s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 1, prometheus.NewRegistry())
		require.NoError(t, err)
		clock := quartz.NewMock(t)
		s.clock = clock
		require.NoError(t, s.Update(
			"tenant1",
			&proto.StreamMetadata{
				StreamHash: 0x1,
				TotalSize:  100,
			},
			clock.Now(),
		))
		// Advance the clock past the active time window.
		clock.Advance(15*time.Minute + 1)
		actual := 0
		for range s.All() {
			actual++
		}
		require.Equal(t, 0, actual)
	})
}

func TestStatsStore_AllForTenant(t *testing.T) {
	t.Run("iterates all streams for tenant", func(t *testing.T) {
		s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 10, prometheus.NewRegistry())
		require.NoError(t, err)
		clock := quartz.NewMock(t)
		s.clock = clock
		for i := 0; i < 10; i++ {
			tenant := "tenant1"
			if i >= 5 {
				tenant = "tenant2"
			}
			// Create 10 streams, one stream for each of the 10 partitions.
			require.NoError(t, s.Update(
				tenant,
				&proto.StreamMetadata{
					StreamHash: uint64(i),
					TotalSize:  100,
				},
				clock.Now(),
			))
		}
		// Check we can iterate the streams for each tenant.
		expected1 := []uint64{0x0, 0x1, 0x2, 0x3, 0x4}
		actual1 := make([]uint64, 0, 5)
		for stream := range s.AllForTenant("tenant1") {
			actual1 = append(actual1, stream.hash)
		}
		require.ElementsMatch(t, expected1, actual1)
		expected2 := []uint64{0x5, 0x6, 0x7, 0x8, 0x9}
		actual2 := make([]uint64, 0, 5)
		for stream := range s.AllForTenant("tenant2") {
			actual2 = append(actual2, stream.hash)
		}
		require.ElementsMatch(t, expected2, actual2)
	})

	t.Run("does not iterate expired streams", func(t *testing.T) {
		s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 1, prometheus.NewRegistry())
		require.NoError(t, err)
		clock := quartz.NewMock(t)
		s.clock = clock
		require.NoError(t, s.Update(
			"tenant1",
			&proto.StreamMetadata{
				StreamHash: 0x1,
				TotalSize:  100,
			},
			clock.Now(),
		))
		// Advance the clock past the active time window.
		clock.Advance(15*time.Minute + 1)
		actual := 0
		for range s.AllForTenant("tenant1") {
			actual++
		}
		require.Equal(t, 0, actual)
	})
}

func TestStatsStore_Update(t *testing.T) {
	s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 1, prometheus.NewRegistry())
	require.NoError(t, err)
	clock := quartz.NewMock(t)
	s.clock = clock
	metadata := &proto.StreamMetadata{
		StreamHash: 0x1,
		TotalSize:  100,
	}
	// Metadata outside the active time window returns an error.
	time1 := clock.Now().Add(-DefaultActiveWindow - 1)
	require.EqualError(t, s.Update("tenant", metadata, time1), "outside active time window")
	// Metadata within the active time window is accepted.
	time2 := clock.Now()
	require.NoError(t, s.Update("tenant", metadata, time2))
}

// This test asserts that we update the correct rate buckets, and as rate
// buckets are implemented as a circular list, when we reach the end of
// list the next bucket is the start of the list.
func TestStatsStore_UpdateRateBuckets(t *testing.T) {
	s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 1, prometheus.NewRegistry())
	require.NoError(t, err)
	clock := quartz.NewMock(t)
	s.clock = clock
	metadata := &proto.StreamMetadata{
		StreamHash: 0x1,
		TotalSize:  100,
	}
	// Metadata at clock.Now() should update the first rate bucket because
	// the mocked clock starts at 2024-01-01T00:00:00Z.
	time1 := clock.Now()
	require.NoError(t, s.Update("tenant", metadata, time1))
	stream, ok := s.getForTests("tenant", 0x1)
	require.True(t, ok)
	expected := newRateBuckets(5*time.Minute, time.Minute)
	expected[0].timestamp = time1.UnixNano()
	expected[0].size = 100
	require.Equal(t, expected, stream.rateBuckets)
	// Update the first bucket with the same metadata but 1 second later.
	clock.Advance(time.Second)
	time2 := clock.Now()
	require.NoError(t, s.Update("tenant", metadata, time2))
	expected[0].size = 200
	require.Equal(t, expected, stream.rateBuckets)
	// Advance the clock forward to the next bucket. Should update the second
	// bucket and leave the first bucket unmodified.
	clock.Advance(time.Minute)
	time3 := clock.Now()
	require.NoError(t, s.Update("tenant", metadata, time3))
	stream, ok = s.getForTests("tenant", 0x1)
	require.True(t, ok)
	// As the clock is now 1 second ahead of the bucket start time, we must
	// truncate the expected time to the start of the bucket.
	expected[1].timestamp = time3.Truncate(time.Minute).UnixNano()
	expected[1].size = 100
	require.Equal(t, expected, stream.rateBuckets)
	// Advance the clock to the last bucket.
	clock.Advance(3 * time.Minute)
	time4 := clock.Now()
	require.NoError(t, s.Update("tenant", metadata, time4))
	stream, ok = s.getForTests("tenant", 0x1)
	require.True(t, ok)
	expected[4].timestamp = time4.Truncate(time.Minute).UnixNano()
	expected[4].size = 100
	require.Equal(t, expected, stream.rateBuckets)
	// Advance the clock one last one. It should wrap around to the start of
	// the list and replace the original bucket with time1.
	clock.Advance(time.Minute)
	time5 := clock.Now()
	require.NoError(t, s.Update("tenant", metadata, time5))
	stream, ok = s.getForTests("tenant", 0x1)
	require.True(t, ok)
	expected[0].timestamp = time5.Truncate(time.Minute).UnixNano()
	expected[0].size = 100
	require.Equal(t, expected, stream.rateBuckets)
}

func TestStatsStore_Evict(t *testing.T) {
	s, err := newStatsStore(15*time.Minute, 5*time.Minute, time.Minute, 1, prometheus.NewRegistry())
	require.NoError(t, err)
	clock := quartz.NewMock(t)
	s.clock = clock
	// The stream 0x1 should be evicted when the clock is advanced 15 mins.
	require.NoError(t, s.Update("tenant1", &proto.StreamMetadata{
		StreamHash: 0x1,
	}, clock.Now()))
	// The streams 0x2 and 0x3 should not be evicted as will still be within
	// the active time window after advancing the clock.
	require.NoError(t, s.Update("tenant2", &proto.StreamMetadata{
		StreamHash: 0x2,
	}, clock.Now().Add(5*time.Minute)))
	require.NoError(t, s.Update("tenant2", &proto.StreamMetadata{
		StreamHash: 0x3,
	}, clock.Now().Add(15*time.Minute)))
	// Advance the clock and run an eviction.
	clock.Advance(15*time.Minute + 1)
	s.Evict()
	actual1 := 0
	for range s.AllForTenant("tenant1") {
		actual1++
	}
	require.Equal(t, 0, actual1)
	actual2 := 0
	for range s.AllForTenant("tenant2") {
		actual2++
	}
	require.Equal(t, 2, actual2)
}

func TestStatsStore_EvictPartitions(t *testing.T) {
	// Create a store with 10 partitions.
	s, err := newStatsStore(DefaultActiveWindow, DefaultRateWindow, DefaultBucketSize, 10, prometheus.NewRegistry())
	require.NoError(t, err)
	clock := quartz.NewMock(t)
	s.clock = clock
	// Create 10 streams. Since we use i as the hash, we can expect the
	// streams to be sharded over all 10 partitions.
	for i := 0; i < 10; i++ {
		s.setForTests("tenant", streamStats{hash: uint64(i), lastSeenAt: clock.Now().UnixNano()})
	}
	// Evict the first 5 partitions.
	s.EvictPartitions([]int32{0, 1, 2, 3, 4})
	// The streams for the last 5 partitions should still be present.
	expected := []uint64{5, 6, 7, 8, 9}
	actual := make([]uint64, 0, len(expected))
	for _, stream := range s.All() {
		actual = append(actual, stream.hash)
	}
	require.ElementsMatch(t, expected, actual)
}

func newRateBuckets(rateWindow, bucketSize time.Duration) []rateBucket {
	return make([]rateBucket, int(rateWindow/bucketSize))
}
