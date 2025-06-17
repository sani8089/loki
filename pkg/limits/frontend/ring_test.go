package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/limits"
	"github.com/grafana/loki/v3/pkg/limits/proto"
)

func TestRingGatherer_CheckLimits(t *testing.T) {
	tests := []struct {
		name    string
		request *proto.CheckLimitsRequest
		// Instances contains the complete set of instances that should be
		// mocked. For example, if a test case is expected to make RPC calls
		// to one instance, then just one InstanceDesc is required.
		instances     []ring.InstanceDesc
		numPartitions int
		// The size of the following slices must match len(instances), where
		// each value is another slice containing the expected requests and
		// responses for the instance. While complex, having a slice of slices
		// means we can set the expectations for each instance such that
		// different instances can expect to receive different numbers of
		// requests for each of the RPCs (as an example, retrying unanswered
		// streams on another zone). The response and responseErr slices
		// behave as enums. For each index, there should be either one non-nil
		// response or one non-nil error, but not both.
		getAssignedPartitionsResponses [][]*proto.GetAssignedPartitionsResponse
		// Even if no errors are expected for the GetAssignedPartitions RPC,
		// each per-instance error slice must have a nil error for each
		// expected response. For example []error{nil, nil} when we expect
		// two successful responses.
		getAssignedPartitionsResponseErrs [][]error
		expectedCheckLimitsRequests       [][]*proto.CheckLimitsRequest
		checkLimitsResponses              [][]*proto.CheckLimitsResponse
		// Even if no errors are expected for the CheckLimits RPC, each
		// per-instance error slice must have a nil error for each expected
		// response. For example []error{nil, nil} when we expect two
		// successful responses.
		checkLimitsResponseErrs [][]error
		expected                []*proto.CheckLimitsResponse
		expectedErr             string
	}{{
		// When there are no streams, no RPCs should be sent.
		name: "no streams",
		request: &proto.CheckLimitsRequest{
			Tenant:  "test",
			Streams: nil,
		},
		instances:                         []ring.InstanceDesc{{Addr: "instance-0"}},
		numPartitions:                     1,
		getAssignedPartitionsResponses:    [][]*proto.GetAssignedPartitionsResponse{nil},
		getAssignedPartitionsResponseErrs: [][]error{nil},
		expectedCheckLimitsRequests:       [][]*proto.CheckLimitsRequest{nil},
		checkLimitsResponses:              [][]*proto.CheckLimitsResponse{nil},
		checkLimitsResponseErrs:           [][]error{nil},
	}, {
		// When there is one instance owning all partitions, that instance is
		// responsible for enforcing limits of all streams.
		name: "one stream one instance",
		request: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1, // 0x1 is assigned to partition 0.
				TotalSize:  0x5,
			}},
		},
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}},
		numPartitions: 1,
		getAssignedPartitionsResponses: [][]*proto.GetAssignedPartitionsResponse{{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}},
		getAssignedPartitionsResponseErrs: [][]error{{nil}},
		expectedCheckLimitsRequests: [][]*proto.CheckLimitsRequest{{{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		}}},
		checkLimitsResponses: [][]*proto.CheckLimitsResponse{{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}}},
		checkLimitsResponseErrs: [][]error{{nil}},
		expected: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
	}, {
		// When there are two instances, each instance is responsible for
		// enforcing limits on just the streams that shard to its consumed
		// partitions. But when we have one stream, just one instance
		// should be called to enforce limits.
		name: "one stream two instances",
		request: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1, // 0x1 is assigned to partition 1.
				TotalSize:  0x5,
			}},
		},
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		numPartitions: 2,
		getAssignedPartitionsResponses: [][]*proto.GetAssignedPartitionsResponse{{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}, {{
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}}},
		getAssignedPartitionsResponseErrs: [][]error{{nil}, {nil}},
		expectedCheckLimitsRequests: [][]*proto.CheckLimitsRequest{
			nil, {{
				Tenant: "test",
				Streams: []*proto.StreamMetadata{{
					StreamHash: 0x1,
					TotalSize:  0x5,
				}},
			}},
		},
		checkLimitsResponses: [][]*proto.CheckLimitsResponse{
			nil, {{
				Results: []*proto.CheckLimitsResult{{
					StreamHash: 0x1,
					Reason:     uint32(limits.ReasonMaxStreams),
				}},
			}},
		},
		checkLimitsResponseErrs: [][]error{{nil}, {nil}},
		expected: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
	}, {
		// When there are two streams and two instances, but all streams
		// shard to one partition, just the instance that consumes that
		// partition should be called to enforce limits.
		name: "two streams, two instances, all streams to one partition",
		request: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1, // 0x1 is assigned to partition 1.
				TotalSize:  0x5,
			}, {
				StreamHash: 0x3, // 0x3 is also assigned to partition 1.
				TotalSize:  0x9,
			}},
		},
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		numPartitions: 2,
		getAssignedPartitionsResponses: [][]*proto.GetAssignedPartitionsResponse{{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}, {{
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}}},
		getAssignedPartitionsResponseErrs: [][]error{{nil}, {nil}},
		expectedCheckLimitsRequests: [][]*proto.CheckLimitsRequest{
			nil, {{
				Tenant: "test",
				Streams: []*proto.StreamMetadata{{
					StreamHash: 0x1,
					TotalSize:  0x5,
				}, {
					StreamHash: 0x3,
					TotalSize:  0x9,
				}},
			}},
		},
		checkLimitsResponses: [][]*proto.CheckLimitsResponse{
			nil, {{
				Results: []*proto.CheckLimitsResult{{
					StreamHash: 0x1,
					Reason:     uint32(limits.ReasonMaxStreams),
				}},
			}},
		},
		checkLimitsResponseErrs: [][]error{{nil}, {nil}},
		expected: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
	}, {
		// When there are two streams and two instances, and each stream
		// shards to different partitions, all instances should be called
		// called to enforce limits.
		name: "two streams, two instances, one stream each",
		request: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1, // 0x1 is assigned to partition 1.
				TotalSize:  0x5,
			}, {
				StreamHash: 0x2, // 0x2 is also assigned to partition 0.
				TotalSize:  0x9,
			}},
		},
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		numPartitions: 2,
		getAssignedPartitionsResponses: [][]*proto.GetAssignedPartitionsResponse{{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}, {{
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}}},
		getAssignedPartitionsResponseErrs: [][]error{{nil}, {nil}},
		expectedCheckLimitsRequests: [][]*proto.CheckLimitsRequest{{{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x2,
				TotalSize:  0x9,
			}},
		}}, {{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		}}},
		checkLimitsResponses: [][]*proto.CheckLimitsResponse{{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x2,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}}, {{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}}},
		checkLimitsResponseErrs: [][]error{{nil}, {nil}},
		expected: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}, {
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x2,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
	}, {
		// When one instance returns an error, the streams for that instance
		// are failed.
		name: "two streams, two instances, one instance returns error",
		request: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1, // 0x1 is assigned to partition 1.
				TotalSize:  0x5,
			}, {
				StreamHash: 0x2, // 0x2 is also assigned to partition 0.
				TotalSize:  0x9,
			}},
		},
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		numPartitions: 2,
		getAssignedPartitionsResponses: [][]*proto.GetAssignedPartitionsResponse{{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}, {{
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}}},
		getAssignedPartitionsResponseErrs: [][]error{{nil}, {nil}},
		expectedCheckLimitsRequests: [][]*proto.CheckLimitsRequest{{{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x2,
				TotalSize:  0x9,
			}},
		}}, {{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		}}},
		checkLimitsResponses: [][]*proto.CheckLimitsResponse{{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x2,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}}, nil},
		checkLimitsResponseErrs: [][]error{{nil}, {
			errors.New("an unexpected error occurred"),
		}},
		expected: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x2,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}, {
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonFailed),
			}},
		}},
	}, {
		// When one zone returns an error, the streams for that instance
		// should be checked against the other zone.
		name: "two streams, two zones, one instance each, first zone returns error",
		request: &proto.CheckLimitsRequest{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1, // 0x1 is assigned to partition 1.
				TotalSize:  0x5,
			}},
		},
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}, {
			Addr: "instance-b-0",
			Zone: "b",
		}},
		numPartitions: 1,
		getAssignedPartitionsResponses: [][]*proto.GetAssignedPartitionsResponse{{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}, {{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}}},
		getAssignedPartitionsResponseErrs: [][]error{{nil}, {nil}},
		expectedCheckLimitsRequests: [][]*proto.CheckLimitsRequest{{{
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		}}, {{
			// The instance in zone b should receive the same request as the
			// instance in zone a.
			Tenant: "test",
			Streams: []*proto.StreamMetadata{{
				StreamHash: 0x1,
				TotalSize:  0x5,
			}},
		}}},
		checkLimitsResponses: [][]*proto.CheckLimitsResponse{{nil}, {{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}}},
		checkLimitsResponseErrs: [][]error{{
			errors.New("an unexpected error occurred"),
		}, {
			nil,
		}},
		expected: []*proto.CheckLimitsResponse{{
			Results: []*proto.CheckLimitsResult{{
				StreamHash: 0x1,
				Reason:     uint32(limits.ReasonMaxStreams),
			}},
		}},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set up the mock clients, one for each set of mock RPC responses.
			mockClients := make([]*mockIngestLimitsClient, len(test.instances))
			for i := 0; i < len(test.instances); i++ {
				mockClients[i] = &mockIngestLimitsClient{
					t:                                 t,
					getAssignedPartitionsResponses:    test.getAssignedPartitionsResponses[i],
					getAssignedPartitionsResponseErrs: test.getAssignedPartitionsResponseErrs[i],
					expectedCheckLimitsRequests:       test.expectedCheckLimitsRequests[i],
					checkLimitsResponses:              test.checkLimitsResponses[i],
					checkLimitsResponseErrs:           test.checkLimitsResponseErrs[i],
				}
				t.Cleanup(mockClients[i].Finished)
			}
			readRing, clientPool := newMockRingWithClientPool(t, "test", mockClients, test.instances)
			cache := newNopCache[string, *proto.GetAssignedPartitionsResponse]()
			g := newRingGatherer(readRing, clientPool, test.numPartitions, cache, log.NewNopLogger(), prometheus.NewRegistry())

			// Set a maximum upper bound on the test execution time.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			actual, err := g.CheckLimits(ctx, test.request)
			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
				require.Nil(t, actual)
			} else {
				require.NoError(t, err)
				require.ElementsMatch(t, test.expected, actual)
			}
		})
	}
}

func TestRingStreamUsageGatherer_GetZoneAwarePartitionConsumers(t *testing.T) {
	tests := []struct {
		name                              string
		instances                         []ring.InstanceDesc
		getAssignedPartitionsResponses    []*proto.GetAssignedPartitionsResponse
		getAssignedPartitionsResponseErrs []error
		expected                          map[string]map[int32]string
	}{{
		name: "single zone",
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil},
		expected:                          map[string]map[int32]string{"a": {0: "instance-a-0"}},
	}, {
		name: "two zones",
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}, {
			Addr: "instance-b-0",
			Zone: "b",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil, nil},
		expected: map[string]map[int32]string{
			"a": {0: "instance-a-0"},
			"b": {0: "instance-b-0"},
		},
	}, {
		name: "two zones, subset of partitions in zone b",
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}, {
			Addr: "instance-b-0",
			Zone: "b",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
				1: time.Now().UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil, nil},
		expected: map[string]map[int32]string{
			"a": {0: "instance-a-0", 1: "instance-a-0"},
			"b": {0: "instance-b-0"},
		},
	}, {
		name: "two zones, instance in zone b returns an error",
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}, {
			Addr: "instance-b-0",
			Zone: "b",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
				1: time.Now().UnixNano(),
			},
		}, nil},
		getAssignedPartitionsResponseErrs: []error{nil, errors.New("an unexpected error occurred")},
		expected: map[string]map[int32]string{
			"a": {0: "instance-a-0", 1: "instance-a-0"},
			"b": {},
		},
	}, {
		name: "two zones, all instances return an error",
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}, {
			Addr: "instance-b-0",
			Zone: "b",
		}},
		getAssignedPartitionsResponses:    []*proto.GetAssignedPartitionsResponse{{}, {}},
		getAssignedPartitionsResponseErrs: []error{nil, nil, nil},
		expected:                          map[string]map[int32]string{"a": {}, "b": {}},
	}, {
		name: "two zones, different number of instances per zone",
		instances: []ring.InstanceDesc{{
			Addr: "instance-a-0",
			Zone: "a",
		}, {
			Addr: "instance-a-1",
			Zone: "a",
		}, {
			Addr: "instance-b-0",
			Zone: "b",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
				1: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil, nil, nil},
		expected: map[string]map[int32]string{
			"a": {0: "instance-a-0", 1: "instance-a-1"},
			"b": {0: "instance-b-0", 1: "instance-b-0"},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set up the mock clients, one for each pair of mock RPC responses.
			mockClients := make([]*mockIngestLimitsClient, len(test.instances))
			for i := range test.instances {
				// These test cases assume one request/response per instance.
				mockClients[i] = &mockIngestLimitsClient{
					t:                                 t,
					getAssignedPartitionsResponses:    []*proto.GetAssignedPartitionsResponse{test.getAssignedPartitionsResponses[i]},
					getAssignedPartitionsResponseErrs: []error{test.getAssignedPartitionsResponseErrs[i]},
				}
				t.Cleanup(mockClients[i].Finished)
			}
			// Set up the mocked ring and client pool for the tests.
			readRing, clientPool := newMockRingWithClientPool(t, "test", mockClients, test.instances)
			cache := newNopCache[string, *proto.GetAssignedPartitionsResponse]()
			g := newRingGatherer(readRing, clientPool, 2, cache, log.NewNopLogger(), prometheus.NewRegistry())

			// Set a maximum upper bound on the test execution time.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			result, err := g.getZoneAwarePartitionConsumers(ctx, test.instances)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestRingStreamUsageGatherer_GetPartitionConsumers(t *testing.T) {
	tests := []struct {
		name string
		// Instances contains the complete set of instances that should be mocked.
		// For example, if a test case is expected to make RPC calls to one instance,
		// then just one InstanceDesc is required.
		instances []ring.InstanceDesc
		// The size of the following slices must match len(instances), where each
		// value contains the expected request/response for the instance at the
		// same index in the instances slice. If a request/response is not expected,
		// the value can be set to nil.
		getAssignedPartitionsResponses    []*proto.GetAssignedPartitionsResponse
		getAssignedPartitionsResponseErrs []error
		// The expected result.
		expected map[int32]string
	}{{
		name: "single instance returns its partitions",
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil},
		expected: map[int32]string{
			0: "instance-0",
		},
	}, {
		name: "two instances return their separate partitions",
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil, nil},
		expected: map[int32]string{
			0: "instance-0",
			1: "instance-1",
		},
	}, {
		name: "two instances claim the same partition, latest timestamp wins",
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().Add(-time.Second).UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}},
		getAssignedPartitionsResponseErrs: []error{nil, nil},
		expected: map[int32]string{
			0: "instance-1",
		},
	}, {
		// Even when one instance returns an error it should still return the
		// partitions for all remaining instances.
		name: "two instances, one returns error",
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().Add(-time.Second).UnixNano(),
			},
		}, nil},
		getAssignedPartitionsResponseErrs: []error{
			nil,
			errors.New("an unexpected error occurred"),
		},
		expected: map[int32]string{
			0: "instance-0",
		},
	}, {
		// Even when all instances return an error, it should not return an
		// error.
		name: "all instances return error",
		instances: []ring.InstanceDesc{{
			Addr: "instance-0",
		}, {
			Addr: "instance-1",
		}},
		getAssignedPartitionsResponses: []*proto.GetAssignedPartitionsResponse{nil, nil},
		getAssignedPartitionsResponseErrs: []error{
			errors.New("an unexpected error occurred"),
			errors.New("an unexpected error occurred"),
		},
		expected: map[int32]string{},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set up the mock clients, one for each pair of mock RPC responses.
			mockClients := make([]*mockIngestLimitsClient, len(test.instances))
			for i := range test.instances {
				// These test cases assume one request/response per instance.
				mockClients[i] = &mockIngestLimitsClient{
					t:                                 t,
					getAssignedPartitionsResponses:    []*proto.GetAssignedPartitionsResponse{test.getAssignedPartitionsResponses[i]},
					getAssignedPartitionsResponseErrs: []error{test.getAssignedPartitionsResponseErrs[i]},
				}
				t.Cleanup(mockClients[i].Finished)
			}
			// Set up the mocked ring and client pool for the tests.
			readRing, clientPool := newMockRingWithClientPool(t, "test", mockClients, test.instances)
			cache := newNopCache[string, *proto.GetAssignedPartitionsResponse]()
			g := newRingGatherer(readRing, clientPool, 1, cache, log.NewNopLogger(), prometheus.NewRegistry())

			// Set a maximum upper bound on the test execution time.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			result, err := g.getPartitionConsumers(ctx, test.instances)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestRingStreamUsageGatherer_GetPartitionConsumers_Caching(t *testing.T) {
	// Set up the mock clients.
	req0 := proto.GetAssignedPartitionsResponse{
		AssignedPartitions: map[int32]int64{
			0: time.Now().UnixNano(),
		},
	}
	client0 := mockIngestLimitsClient{
		t: t,
		// Expect the same request twice. The first time on the first
		// cache miss, and the second time on the second cache miss after
		// resetting the cache.
		getAssignedPartitionsResponses:    []*proto.GetAssignedPartitionsResponse{&req0, &req0},
		getAssignedPartitionsResponseErrs: []error{nil, nil},
	}
	t.Cleanup(client0.Finished)
	req1 := proto.GetAssignedPartitionsResponse{
		AssignedPartitions: map[int32]int64{
			1: time.Now().UnixNano(),
		},
	}
	client1 := mockIngestLimitsClient{
		t: t,
		// Expect the same request twice too for the same reasons.
		getAssignedPartitionsResponses:    []*proto.GetAssignedPartitionsResponse{&req1, &req1},
		getAssignedPartitionsResponseErrs: []error{nil, nil},
	}
	t.Cleanup(client1.Finished)
	mockClients := []*mockIngestLimitsClient{&client0, &client1}
	instances := []ring.InstanceDesc{{Addr: "instance-0"}, {Addr: "instance-1"}}

	// Set up the mocked ring and client pool for the tests.
	readRing, clientPool := newMockRingWithClientPool(t, "test", mockClients, instances)

	// Set the cache TTL large enough that entries cannot expire (flake)
	// during slow test runs.
	cache := newTTLCache[string, *proto.GetAssignedPartitionsResponse](time.Minute)
	g := newRingGatherer(readRing, clientPool, 2, cache, log.NewNopLogger(), prometheus.NewRegistry())

	// Set a maximum upper bound on the test execution time.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.Equal(t, 0, client0.numAssignedPartitionsRequests)
	require.Equal(t, 0, client1.numAssignedPartitionsRequests)

	expected := map[int32]string{
		0: "instance-0",
		1: "instance-1",
	}

	// The first call should be a cache miss.
	actual, err := g.getPartitionConsumers(ctx, instances)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, 1, client0.numAssignedPartitionsRequests)
	require.Equal(t, 1, client1.numAssignedPartitionsRequests)

	// The second call should be a cache hit.
	actual, err = g.getPartitionConsumers(ctx, instances)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, 1, client0.numAssignedPartitionsRequests)
	require.Equal(t, 1, client1.numAssignedPartitionsRequests)

	// Expire the cache, it should be a cache miss.
	cache.Reset()

	// The third call should be a cache miss.
	actual, err = g.getPartitionConsumers(ctx, instances)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, 2, client0.numAssignedPartitionsRequests)
	require.Equal(t, 2, client1.numAssignedPartitionsRequests)
}
