package dlq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/testing/protorequire"
)

type testMergeClient struct {
	segmentClient
	read func(context.Context, historydlq.MessageRef) (historydlq.StoredMessage, error)
}

func (c testMergeClient) ReadMessage(ctx context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
	return c.read(ctx, ref)
}

func mergeFixture(t *testing.T) (historydlq.SegmentRef, []historydlq.MessageRef) {
	t.Helper()
	_, segment := segmentJobFixture(t)
	refs := []historydlq.MessageRef{{
		Queue: segment.Queue, BusinessID: segment.BusinessID, RunID: segment.RunID,
		MessageID: 98, EntryKey: "entry-98", EnqueuedAt: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC),
	}}
	second := refs[0]
	second.EntryKey, second.MessageID = "entry-99", 99
	return segment, append(refs, second)
}

func mergeStoredMessage(ref historydlq.MessageRef) historydlq.StoredMessage {
	return historydlq.StoredMessage{
		Ref:                ref,
		SourceTaskIdentity: historydlq.TaskIdentity{SourceShardID: 7},
		Task: &commonspb.HistoryTask{
			ShardId: 7,
			Blob:    &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("payload-" + ref.EntryKey)},
		},
	}
}

func TestMergeResolvesSuccessfulEntries(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"success", "first submission fails", "tied IDs"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			segment, refs := mergeFixture(t)
			if scenario == "tied IDs" {
				refs[1].MessageID = refs[0].MessageID
			}
			var events []string
			var resolveRequests []historydlq.ResolveEntriesRequest
			component := &workerComponent{
				currentClusterName: "target",
				chasmClient: testMergeClient{
					segmentClient: testPurgeClient{resolve: func(ctx context.Context, req historydlq.ResolveEntriesRequest) error {
						require.Equal(t, headers.CallerTypePreemptable, headers.GetCallerInfo(ctx).CallerType)
						events = append(events, "resolve")
						resolveRequests = append(resolveRequests, req)
						return nil
					}},
					read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
						events = append(events, "read:"+ref.EntryKey)
						return mergeStoredMessage(ref), nil
					},
				},
				taskClientDialer: TaskClientDialerFn(func(_ context.Context, cluster string) (TaskClient, error) {
					require.Equal(t, "source", cluster)
					return AddTasksFn(func(_ context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
						events = append(events, "add-batch")
						protorequire.ProtoEqual(t, &adminservice.AddTasksRequest{
							ShardId: 7, ReturnTaskFailures: true,
							Tasks: []*adminservice.AddTasksRequest_Task{
								{CategoryId: segment.Queue.CategoryID, Blob: mergeStoredMessage(refs[0]).Task.Blob},
								{CategoryId: segment.Queue.CategoryID, Blob: mergeStoredMessage(refs[1]).Task.Blob},
							},
						}, req)
						if scenario == "first submission fails" {
							return &adminservice.AddTasksResponse{Failures: []*commonspb.AddTaskFailure{
								{TaskIndex: 0, Message: "submission uncertain"},
							}}, nil
						}
						return &adminservice.AddTasksResponse{}, nil
					}), nil
				}),
			}
			result, err := component.mergeEntries(headers.SetCallerType(t.Context(), headers.CallerTypePreemptable), MergeEntriesParams{
				Segment: segment, Refs: append(append([]historydlq.MessageRef{}, refs...), refs[0]),
			})
			require.NoError(t, err)
			expected := MergeEntriesResult{ResolvedCount: 2}
			keys := []string{refs[0].EntryKey, refs[1].EntryKey}
			if scenario == "first submission fails" {
				expected.ResolvedCount = 1
				expected.Failures = []MergeEntryFailure{{Ref: refs[0], Error: "submission uncertain"}}
				keys = keys[1:]
			}
			require.Equal(t, expected, result)
			require.Equal(t, []historydlq.ResolveEntriesRequest{{Segment: segment, EntryKeys: keys, Resolution: historydlqpb.RESOLUTION_MERGED}}, resolveRequests)
			require.Equal(t, []string{"read:entry-98", "read:entry-99", "add-batch", "resolve"}, events)
		})
	}
}

func TestMergeEntriesValidation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"remote queue", "wrong segment", "wrong run", "wrong queue", "empty key"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			segment, refs := mergeFixture(t)
			switch scenario {
			case "remote queue":
				segment.Queue.TargetCluster = "remote"
			case "wrong segment":
				refs[1].BusinessID = "other"
			case "wrong run":
				refs[1].RunID = "other"
			case "wrong queue":
				refs[1].Queue.SourceCluster = "other"
			case "empty key":
				refs[1].EntryKey = ""
			default:
				t.Fatalf("unknown scenario: %s", scenario)
			}
			component := &workerComponent{currentClusterName: "target"}
			_, err := component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: refs})
			var applicationError *temporal.ApplicationError
			require.ErrorAs(t, err, &applicationError)
			require.True(t, applicationError.NonRetryable())
		})
	}
}

func TestMergeReadFailures(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testMergeClient{read: func(context.Context, historydlq.MessageRef) (historydlq.StoredMessage, error) {
			return historydlq.StoredMessage{}, serviceerror.NewDataLoss("pending DATA missing")
		}},
	}
	result, err := component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{Failures: []MergeEntryFailure{
		{Ref: refs[0], Error: "pending DATA missing"}, {Ref: refs[1], Error: "pending DATA missing"},
	}}, result)
}

func TestMergeResolveChunks(t *testing.T) {
	t.Parallel()
	segment, initialRefs := mergeFixture(t)
	var refs []historydlq.MessageRef
	for i := range historydlq.MaxResolveBatchSize + 1 {
		ref := initialRefs[0]
		ref.EntryKey = fmt.Sprintf("entry-%d", i)
		refs = append(refs, ref)
	}
	var batchSizes []int
	var submissionSizes []int
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testMergeClient{
			segmentClient: testPurgeClient{resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
				batchSizes = append(batchSizes, len(req.EntryKeys))
				if len(batchSizes) == 1 {
					return serviceerror.NewUnavailable("resolve uncertain")
				}
				return nil
			}},
			read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
				return mergeStoredMessage(ref), nil
			},
		},
		taskClientDialer: TaskClientDialerFn(func(context.Context, string) (TaskClient, error) {
			return AddTasksFn(func(_ context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
				submissionSizes = append(submissionSizes, len(req.Tasks))
				return &adminservice.AddTasksResponse{}, nil
			}), nil
		}),
	}
	result, err := component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, []int{historydlq.MaxResolveBatchSize, 1}, batchSizes)
	require.Equal(t, []int{MaxMergeBatchSize, 1}, submissionSizes)
	var expected []MergeEntryFailure
	for i, ref := range refs {
		outcome := MergeEntryFailure{Ref: ref, Error: "resolve uncertain"}
		if i == historydlq.MaxResolveBatchSize {
			continue
		}
		expected = append(expected, outcome)
	}
	require.Equal(t, MergeEntriesResult{Failures: expected, ResolvedCount: 1}, result)
}

func TestMergeCancelledBeforeResolve(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	resolveCalls := 0
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testMergeClient{
			segmentClient: testPurgeClient{resolve: func(context.Context, historydlq.ResolveEntriesRequest) error {
				resolveCalls++
				return nil
			}},
			read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
				return mergeStoredMessage(ref), nil
			},
		},
		taskClientDialer: TaskClientDialerFn(func(context.Context, string) (TaskClient, error) {
			return AddTasksFn(func(context.Context, *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
				cancel()
				return &adminservice.AddTasksResponse{}, nil
			}), nil
		}),
	}
	result, err := component.mergeEntries(ctx, MergeEntriesParams{Segment: segment, Refs: refs[:1]})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, MergeEntriesResult{}, result)
	require.Zero(t, resolveCalls)
}

func TestMergeInvalidBatchFailures(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"negative index", "out of range", "nil failure", "nil response"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			segment, refs := mergeFixture(t)
			response := &adminservice.AddTasksResponse{}
			message := "AddTasks returned an invalid failed task index"
			switch scenario {
			case "negative index":
				response.Failures = []*commonspb.AddTaskFailure{{TaskIndex: -1}}
			case "out of range":
				response.Failures = []*commonspb.AddTaskFailure{{TaskIndex: 2}}
			case "nil failure":
				response.Failures = []*commonspb.AddTaskFailure{nil}
			case "nil response":
				response = nil
				message = "AddTasks returned no response"
			default:
				t.Fatalf("unknown scenario: %s", scenario)
			}
			calls := 0
			component := &workerComponent{
				currentClusterName: "target",
				chasmClient: testMergeClient{read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
					return mergeStoredMessage(ref), nil
				}},
				taskClientDialer: TaskClientDialerFn(func(context.Context, string) (TaskClient, error) {
					return AddTasksFn(func(context.Context, *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
						calls++
						return response, nil
					}), nil
				}),
			}
			result, err := component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: refs})
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, MergeEntriesResult{Failures: []MergeEntryFailure{
				{Ref: refs[0], Error: message}, {Ref: refs[1], Error: message},
			}}, result)
		})
	}
}

func TestMergeFailureIndexesAfterReadFailures(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	third := refs[1]
	third.EntryKey, third.MessageID = "entry-100", 100
	refs = append(refs, third)
	var resolvedKeys []string
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testMergeClient{
			segmentClient: testPurgeClient{resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
				resolvedKeys = append(resolvedKeys, req.EntryKeys...)
				return nil
			}},
			read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
				if ref.EntryKey == refs[0].EntryKey {
					return historydlq.StoredMessage{}, serviceerror.NewDataLoss("payload missing")
				}
				return mergeStoredMessage(ref), nil
			},
		},
		taskClientDialer: TaskClientDialerFn(func(context.Context, string) (TaskClient, error) {
			return AddTasksFn(func(_ context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
				require.Len(t, req.Tasks, 2)
				return &adminservice.AddTasksResponse{Failures: []*commonspb.AddTaskFailure{
					{TaskIndex: 1, Message: "last task rejected", NonRetryable: true},
				}}, nil
			}), nil
		}),
	}
	result, err := component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 1, Failures: []MergeEntryFailure{
		{Ref: refs[0], Error: "payload missing"},
		{Ref: refs[2], Error: "last task rejected", NonRetryable: true},
	}}, result)
	require.Equal(t, []string{refs[1].EntryKey}, resolvedKeys)
}
