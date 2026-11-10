package dlq

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
)

type mergeRecoveryFixture struct {
	state               *purgeFixture
	component           *workerComponent
	reads               []historydlq.MessageRef
	listings            []historydlq.ListEntriesRequest
	accepted            []historydlq.MessageRef
	resolves            []historydlq.ResolveEntriesRequest
	pendingReads        []historydlq.MessageRef
	failFirstTask       bool
	loseAddResponse     bool
	loseResolveResponse bool
	rejectResolve       bool
}

func newMergeRecoveryFixture(t *testing.T) *mergeRecoveryFixture {
	t.Helper()
	fixture := &mergeRecoveryFixture{state: newPurgeFixture(t)}
	fixture.component = &workerComponent{
		currentClusterName: "target",
		chasmClient: testMergeClient{
			segmentClient: testPurgeClient{
				segmentClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
					fixture.listings = append(fixture.listings, req)
					return fixture.state.segment.ListEntries(fixture.state.ctx, req)
				}},
				resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
					fixture.resolves = append(fixture.resolves, req)
					if fixture.rejectResolve {
						fixture.rejectResolve = false
						return serviceerror.NewUnavailable("resolve rejected")
					}
					if err := fixture.state.segment.ResolveEntries(fixture.state.ctx, req); err != nil {
						return err
					}
					if fixture.loseResolveResponse {
						fixture.loseResolveResponse = false
						return serviceerror.NewUnavailable("resolve response lost")
					}
					return nil
				},
			},
			read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
				fixture.reads = append(fixture.reads, ref)
				stored, err := fixture.state.segment.ReadEntry(fixture.state.ctx, ref)
				if err == nil {
					fixture.pendingReads = append(fixture.pendingReads, stored.Ref)
				}
				return stored, err
			},
		},
		taskClientDialer: TaskClientDialerFn(func(context.Context, string) (TaskClient, error) {
			return AddTasksFn(func(_ context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
				fixture.accepted = append(fixture.accepted, fixture.pendingReads[:len(req.Tasks)]...)
				fixture.pendingReads = fixture.pendingReads[len(req.Tasks):]
				if fixture.loseAddResponse {
					fixture.loseAddResponse = false
					return nil, serviceerror.NewUnavailable("AddTasks response lost after persistence")
				}
				if fixture.failFirstTask {
					fixture.failFirstTask = false
					return &adminservice.AddTasksResponse{Failures: []*commonspb.AddTaskFailure{
						{TaskIndex: 0, Message: "task persistence uncertain"},
					}}, nil
				}
				return &adminservice.AddTasksResponse{}, nil
			}), nil
		}),
	}
	return fixture
}

func TestMergeSkipsDeletedEntry(t *testing.T) {
	t.Parallel()
	fixture := newMergeRecoveryFixture(t)
	ref := fixture.state.refs[0]
	require.NoError(t, fixture.state.segment.ResolveEntries(fixture.state.ctx, historydlq.ResolveEntriesRequest{
		Segment: fixture.state.ref, EntryKeys: []string{ref.EntryKey}, Resolution: historydlqpb.RESOLUTION_PURGED,
	}))
	result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: []historydlq.MessageRef{ref}})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 1}, result)
	require.Empty(t, fixture.accepted)
	require.Empty(t, fixture.resolves)
	require.Len(t, fixture.listings, 1)
	require.Equal(t, int64(math.MaxInt64), fixture.listings[0].InclusiveMaxMessageID)
}

func TestMergeSelectedRecreation(t *testing.T) {
	t.Parallel()
	fixture := newMergeRecoveryFixture(t)
	ref := fixture.state.refs[0]
	require.NoError(t, fixture.state.segment.ResolveEntries(fixture.state.ctx, historydlq.ResolveEntriesRequest{
		Segment: fixture.state.ref, EntryKeys: []string{ref.EntryKey}, Resolution: historydlqpb.RESOLUTION_PURGED,
	}))
	fixture.state.now = fixture.state.now.Add(time.Hour)
	accepted, err := fixture.state.segment.Append(fixture.state.ctx, fixture.state.request)
	require.NoError(t, err)
	require.NotEqual(t, ref.MessageID, accepted.MessageRef.MessageID)
	result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: []historydlq.MessageRef{ref}})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 1}, result)
	require.Equal(t, []historydlq.MessageRef{ref, accepted.MessageRef}, fixture.reads)
	require.Equal(t, []historydlq.MessageRef{accepted.MessageRef}, fixture.accepted)
	require.NotContains(t, fixture.state.segment.Entries, ref.EntryKey)
}

func TestMergeLostAddTasksResponse(t *testing.T) {
	t.Parallel()
	fixture := newMergeRecoveryFixture(t)
	refs := fixture.state.refs[:2]
	fixture.loseAddResponse = true
	result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{Failures: []MergeEntryFailure{
		{Ref: refs[0], Error: "AddTasks response lost after persistence"},
		{Ref: refs[1], Error: "AddTasks response lost after persistence"},
	}}, result)
	require.Equal(t, refs, fixture.accepted)
	require.Contains(t, fixture.state.segment.Entries, refs[0].EntryKey)
	require.Contains(t, fixture.state.segment.Entries, refs[1].EntryKey)
	require.Empty(t, fixture.resolves)
	result, err = fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 2}, result)
	require.Equal(t, []historydlq.MessageRef{refs[0], refs[1], refs[0], refs[1]}, fixture.accepted)
	require.Len(t, fixture.state.segment.Entries, 1)
}

func TestMergePartialBatchPersistence(t *testing.T) {
	t.Parallel()
	fixture := newMergeRecoveryFixture(t)
	refs := fixture.state.refs[:2]
	fixture.failFirstTask = true
	result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 1, Failures: []MergeEntryFailure{
		{Ref: refs[0], Error: "task persistence uncertain"},
	}}, result)
	require.Contains(t, fixture.state.segment.Entries, refs[0].EntryKey)
	require.NotContains(t, fixture.state.segment.Entries, refs[1].EntryKey)
	result, err = fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs[:1]})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 1}, result)
	require.Equal(t, []historydlq.MessageRef{refs[0], refs[1], refs[0]}, fixture.accepted)
}

func TestMergeResolveRetry(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"lost response", "not committed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			fixture := newMergeRecoveryFixture(t)
			refs := fixture.state.refs[:2]
			if scenario == "lost response" {
				fixture.loseResolveResponse = true
			} else {
				fixture.rejectResolve = true
			}
			result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs})
			require.NoError(t, err)
			require.Len(t, result.Failures, 2)
			for _, outcome := range result.Failures {
				require.NotEmpty(t, outcome.Error)
				require.False(t, outcome.NonRetryable)
			}
			result, err = fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs})
			require.NoError(t, err)
			require.Equal(t, MergeEntriesResult{ResolvedCount: 2}, result)
			expectedAccepted := append([]historydlq.MessageRef{}, refs...)
			if scenario == "not committed" {
				expectedAccepted = append(expectedAccepted, refs...)
			}
			require.Equal(t, expectedAccepted, fixture.accepted)
			require.Len(t, fixture.state.segment.Entries, 1)
		})
	}
}

func TestMergeCorruptPayloadStaysPending(t *testing.T) {
	t.Parallel()
	fixture := newMergeRecoveryFixture(t)
	refs := fixture.state.refs[:2]
	fixture.state.segment.Entries[refs[0].EntryKey].Get(fixture.state.ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
	result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: fixture.state.ref, Refs: refs})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{ResolvedCount: 1, Failures: []MergeEntryFailure{
		{Ref: refs[0], Error: "history DLQ entry is missing its task"},
	}}, result)
	require.Equal(t, []historydlq.MessageRef{refs[1]}, fixture.accepted)
	require.Contains(t, fixture.state.segment.Entries, refs[0].EntryKey)
	require.Empty(t, fixture.listings)
}

func TestMergeMissingRun(t *testing.T) {
	t.Parallel()
	fixture := newMergeRecoveryFixture(t)
	ref := fixture.state.refs[0]
	ref.RunID = "missing-run"
	segment := fixture.state.ref
	segment.RunID = ref.RunID
	result, err := fixture.component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: []historydlq.MessageRef{ref}})
	require.NoError(t, err)
	require.Zero(t, result.ResolvedCount)
	require.True(t, result.Failures[0].NonRetryable)
	require.NotEmpty(t, result.Failures[0].Error)
	require.Len(t, fixture.listings, 1)
	require.Empty(t, fixture.accepted)
	require.Empty(t, fixture.resolves)
}

func TestMergeRefreshOnce(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	current := refs[0]
	current.MessageID++
	var reads []historydlq.MessageRef
	var tokens []string
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testMergeClient{
			segmentClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
				require.Equal(t, segment, req.Segment)
				require.Equal(t, int64(math.MaxInt64), req.InclusiveMaxMessageID)
				tokens = append(tokens, string(req.NextPageToken))
				if len(req.NextPageToken) == 0 {
					return historydlq.EntryPage{NextPageToken: []byte("next")}, nil
				}
				return historydlq.EntryPage{Refs: []historydlq.MessageRef{current}}, nil
			}},
			read: func(_ context.Context, ref historydlq.MessageRef) (historydlq.StoredMessage, error) {
				reads = append(reads, ref)
				return historydlq.StoredMessage{}, serviceerror.NewFailedPrecondition("reference changed again")
			},
		},
	}
	result, err := component.mergeEntries(t.Context(), MergeEntriesParams{Segment: segment, Refs: refs[:1]})
	require.NoError(t, err)
	require.Equal(t, MergeEntriesResult{Failures: []MergeEntryFailure{{Ref: refs[0], Error: "reference changed again"}}}, result)
	require.Equal(t, []historydlq.MessageRef{refs[0], current}, reads)
	require.Equal(t, []string{"", "next"}, tokens)
}
