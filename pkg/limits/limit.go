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

// Limits contains all limits enforced by the limits frontend.
type Limits interface {
	IngestionRateBytes(userID string) float64
	IngestionBurstSizeBytes(userID string) int
	MaxGlobalStreamsPerUser(userID string) int
}

type limitsChecker struct {
	limits           Limits
	store            *statsStore
	producer         *producer
	partitionManager *partitionManager
	numPartitions    int
	logger           log.Logger

	// Metrics.
	tenantIngestedBytesTotal *prometheus.CounterVec

	// Used in tests.
	clock quartz.Clock
}

func newLimitsChecker(limits Limits, store *statsStore, producer *producer, partitionManager *partitionManager, numPartitions int, logger log.Logger, reg prometheus.Registerer) *limitsChecker {
	return &limitsChecker{
		limits:           limits,
		store:            store,
		producer:         producer,
		partitionManager: partitionManager,
		numPartitions:    numPartitions,
		logger:           logger,
		tenantIngestedBytesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "loki_ingest_limits_tenant_ingested_bytes_total",
			Help: "Total number of bytes ingested per tenant within the active window. This is not a global total, as tenants can be sharded over multiple pods.",
		}, []string{"tenant"}),
		clock: quartz.NewReal(),
	}
}

func (c *limitsChecker) ExceedsLimits(ctx context.Context, req *proto.ExceedsLimitsRequest) (*proto.ExceedsLimitsResponse, error) {
	var (
		maxStreams          = c.limits.MaxGlobalStreamsPerUser(req.Tenant)
		partitionMaxStreams = maxStreams / c.numPartitions
		accepted            = make([]*proto.StreamMetadata, 0, len(req.Streams))
		rejected            = make([]*proto.StreamMetadata, 0, len(req.Streams))
	)
	c.store.ForTenant(req.Tenant, func(e tenantStatsEntry) {
		for _, metadata := range req.Streams {
			partition := int32(metadata.StreamHash % uint64(c.numPartitions))
			// TODO(grobinson): What to do in this case?
			if !c.partitionManager.Has(partition) {
				level.Warn(c.logger).Log(
					"msg",
					"received stream for partition not assigned to us, dropping it",
					"stream",
					metadata.StreamHash,
					"partition",
					partition,
				)
				continue
			}
			stats, ok := e.Get(metadata.StreamHash)
			if !ok {
				// if ok {
				// 	// The stream has expired, delete it so it doesn't count
				// 	// towards the active streams.
				// 	e.Delete(stream.StreamHash)
				// }
				// Get the total number of streams, including expired
				// streams. While we would like to count just the number of
				// active streams, this would mean iterating all streams
				// in the partition which is O(N) instead of O(1). Instead,
				// we accept that expired streams will be counted towards the
				// limit until evicted.
				if e.CountPartitionStreams(partition) >= partitionMaxStreams {
					rejected = append(rejected, metadata)
					continue
				}
				accepted = append(accepted, metadata)
				e.Update(metadata, seenAt)
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

	c.tenantIngestedBytesTotal.WithLabelValues(req.Tenant).Add(float64(ingestedBytes))

	results := make([]*proto.ExceedsLimitsResult, 0, len(rejected))
	for _, stream := range rejected {
		results = append(results, &proto.ExceedsLimitsResult{
			StreamHash: stream.StreamHash,
			Reason:     uint32(ReasonExceedsMaxStreams),
		})
	}

	return &proto.ExceedsLimitsResponse{Results: results}, nil
}
