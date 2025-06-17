package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/limits"
	"github.com/grafana/loki/v3/pkg/limits/proto"
)

func TestFrontend_CheckLimits(t *testing.T) {
	tests := []struct {
		name                 string
		checkLimitsRequest   *proto.CheckLimitsRequest
		checkLimitsResponses []*proto.CheckLimitsResponse
		err                  error
		expected             *proto.CheckLimitsResponse
	}{{
		name: "no streams",
		checkLimitsRequest: &proto.CheckLimitsRequest{
			Tenant:  "test",
			Streams: nil,
		},
		expected: &proto.CheckLimitsResponse{
			Results: []*proto.CheckLimitsResult{},
		},
	}, {
		name: "one stream",
		checkLimitsRequest: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		},
		checkLimitsResponses: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
		expected: &proto.CheckLimitsResponse{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		},
	}, {
		name: "one stream, no responses",
		checkLimitsRequest: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		},
		checkLimitsResponses: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{},
		}},
		expected: &proto.CheckLimitsResponse{
			Results: []*proto.CheckLimitsResult{},
		},
	}, {
		name: "two stream, one response",
		checkLimitsRequest: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}, {
				StreamHash: 0x4,
				TotalSize:  0x9,
			}},
		},
		checkLimitsResponses: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}, {
				StreamHash: 0x4,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
		expected: &proto.CheckLimitsResponse{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}, {
				StreamHash: 0x4,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		},
	}, {
		name: "two stream, two responses",
		checkLimitsRequest: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}, {
				StreamHash: 0x4,
				TotalSize:  0x9,
			}},
		},
		checkLimitsResponses: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}, {
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x4,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
		expected: &proto.CheckLimitsResponse{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}, {
				StreamHash: 0x4,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		},
	}, {
		name: "unexpected error, response with failed reason",
		checkLimitsRequest: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}, {
				StreamHash: 0x2,
				TotalSize:  0x9,
			}},
		},
		err: errors.New("an unexpected error occurred"),
		expected: &proto.CheckLimitsResponse{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonFailed),
			}, {
				StreamHash: 0x2,
				Reason:     uint32(limits.ReasonFailed),
			}},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			readRing, _ := newMockRingWithClientPool(t, "test", nil, nil)
			f, err := New(Config{
				LifecyclerConfig: ring.LifecyclerConfig{
					RingConfig: ring.Config{
						KVStore: kv.Config{
							Store: "inmemory",
						},
					},
				},
			}, "test", readRing, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)
			// Replace with our mock.
			f.gatherer = &mockCheckLimitsGatherer{
				t:                          t,
				expectedCheckLimitsRequest: test.checkLimitsRequest,
				checkLimitsResponses:       test.checkLimitsResponses,
				err:                        test.err,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			actual, err := f.CheckLimits(ctx, test.checkLimitsRequest)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}
