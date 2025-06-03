package limits

import (
	"context"

	"github.com/coder/quartz"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/loki/v3/pkg/limits/proto"
)

// The Limits interface contains all supported per-tenant limits.
type Limits interface {
	IngestionRateBytes(userID string) float64
	IngestionBurstSizeBytes(userID string) int
	MaxGlobalStreamsPerUser(userID string) int
}

// A limitsChecker accepts ExceedsLimitsRequests, and for each stream in the
// request, checks if it is within the per-tenant limits.
type limitsChecker struct {
	limits           Limits
	partitionManager *partitionManager
	numPartitions    int
	producer         *producer
	store            *statsStore
	logger           log.Logger

	// Metrics.
	ingestedBytesTotal *prometheus.CounterVec

	// Used in tests.
	clock quartz.Clock
}

// newLimitsChecker returns a new limitsChecker.
func newLimitsChecker(
	limits Limits,
	partitionManager *partitionManager,
	numPartitions int,
	producer *producer,
	store *statsStore,
	logger log.Logger,
	reg prometheus.Registerer,
) *limitsChecker {
	return &limitsChecker{
		limits:           limits,
		partitionManager: partitionManager,
		numPartitions:    numPartitions,
		producer:         producer,
		store:            store,
		logger:           logger,
		ingestedBytesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "loki_ingest_limits_tenant_ingested_bytes_total",
			Help: "Total number of bytes ingested per tenant within the active window. This is not a global total, as tenants can be sharded over multiple pods.",
		}, []string{"tenant"}),
		clock: quartz.NewReal(),
	}
}

func (c *limitsChecker) ExceedsLimits(ctx context.Context, req *proto.ExceedsLimitsRequest) (*proto.ExceedsLimitsResponse, error) {
	var (
		// The max stream limit is a global limit. However, as limits are
		// enforced per-partition we need to calculate the per-partition
		// stream limit.
		maxStreams          = c.limits.MaxGlobalStreamsPerUser(req.Tenant)
		partitionMaxStreams = maxStreams / c.numPartitions
		// accepted contains the streams from the request that are within the
		// per-tenant limits. These will be pushed to Kafka to be replicated
		// to other zones.
		accepted = make([]*proto.StreamMetadata, 0, len(req.Streams))
		// rejected contains the streams from the request that exceed the
		// per-tenant limits and must be rejected. These will be returned
		// as results in the response.
		rejected = make([]*proto.StreamMetadata, 0, len(req.Streams))
	)
	c.store.WithTenant(req.Tenant, func(s *tenantStatsStore) {
		for _, metadata := range req.Streams {
			partition := int32(metadata.StreamHash % uint64(c.numPartitions))
			if !c.partitionManager.Has(partition) {
				// TODO(grobinson): We need to figure out what to do in this
				// case.
				level.Warn(c.logger).Log(
					"msg",
					"dropped stream as its partition is not assigned to us",
					"stream",
					metadata.StreamHash,
					"partition",
					partition,
				)
				continue
			}
			// Check if the stream exists or has expired.
			if !s.Has(metadata.StreamHash) {
				numStreams := s.CountPartitionStreams(partition)
				if s.IsExpired(metadata.StreamHash) {
					// The stream has expired, we might be able to re-create
					// it if we are just at the limit.
					numStreams--
				}
				// Get the total number of streams, including expired
				// streams. While we would like to count just the number of
				// active streams, this would mean iterating all streams
				// in the partition which is O(N) instead of O(1). Instead,
				// we accept that expired streams will be counted towards the
				// limit until evicted.
				if numStreams >= partitionMaxStreams {
					rejected = append(rejected, metadata)
					continue
				}
				accepted = append(accepted, metadata)
				s.Update(metadata.StreamHash, metadata.TotalSize, c.clock.Now())
			}
		}
	})

	var ingestedBytes uint64
	for _, stream := range accepted {
		ingestedBytes += stream.TotalSize
		err := c.producer.Produce(context.WithoutCancel(ctx), req.Tenant, stream)
		if err != nil {
			level.Error(c.logger).Log("msg", "failed to send streams", "error", err)
		}
	}

	c.ingestedBytesTotal.WithLabelValues(req.Tenant).Add(float64(ingestedBytes))

	results := make([]*proto.ExceedsLimitsResult, 0, len(rejected))
	for _, stream := range rejected {
		results = append(results, &proto.ExceedsLimitsResult{
			StreamHash: stream.StreamHash,
			Reason:     uint32(ReasonExceedsMaxStreams),
		})
	}

	return &proto.ExceedsLimitsResponse{Results: results}, nil
}
