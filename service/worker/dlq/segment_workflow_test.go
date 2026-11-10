package dlq

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
)

func TestMixedMergeRetriesOnlyFailures(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
	env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{}, nil)
	var mu sync.Mutex
	var events []string
	var attempts []MergeEntriesParams
	var sdkAttempts []int32
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, "list:"+string(req.NextPageToken))
			if len(req.NextPageToken) == 0 {
				return historydlq.EntryPage{Refs: refs, NextPageToken: []byte("next")}, nil
			}
			return historydlq.EntryPage{}, nil
		})
	env.OnActivity(mergeEntriesActivityName, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, params MergeEntriesParams) (MergeEntriesResult, error) {
			mu.Lock()
			defer mu.Unlock()
			attempts = append(attempts, params)
			sdkAttempts = append(sdkAttempts, activity.GetInfo(ctx).Attempt)
			events = append(events, "merge")
			if len(attempts) == 1 {
				return MergeEntriesResult{ResolvedCount: 1, Failures: []MergeEntryFailure{{Ref: refs[0], Error: "retry"}}}, nil
			}
			return MergeEntriesResult{ResolvedCount: 1}, nil
		})
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{
		Key: Key{TaskCategoryID: int(segment.Queue.CategoryID), SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100, BatchSize: 2,
	}})
	require.NoError(t, env.GetWorkflowError())
	encoded, err := env.QueryWorkflow(QueryTypeProgress)
	require.NoError(t, err)
	var progress ProgressQueryResponse
	require.NoError(t, encoded.Get(&progress))
	require.Equal(t, int64(2), progress.NumberOfMessagesProcessed)
	require.Equal(t, int64(99), progress.LastProcessedMessageID)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []MergeEntriesParams{{Segment: segment, Refs: refs}, {Segment: segment, Refs: refs[:1]}}, attempts)
	require.Equal(t, []int32{1, 1}, sdkAttempts)
	require.Equal(t, []string{"list:", "merge", "merge", "list:next"}, events)
}

func TestMixedMergeExhaustionContinuesPages(t *testing.T) {
	t.Parallel()
	for _, permanent := range []bool{false, true} {
		name := "retry exhaustion"
		if permanent {
			name = "permanent failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			segment, refs := mergeFixture(t)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			component := &workerComponent{}
			component.RegisterWorkflow(env)
			component.RegisterActivities(env)
			env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
			env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{}, nil)
			env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(
				func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
					if len(req.NextPageToken) == 0 {
						return historydlq.EntryPage{Refs: refs[:1], NextPageToken: []byte("next")}, nil
					}
					return historydlq.EntryPage{Refs: refs[1:]}, nil
				})
			var mu sync.Mutex
			failedCalls, successfulCalls := 0, 0
			env.OnActivity(mergeEntriesActivityName, mock.Anything, mock.Anything).Return(
				func(_ context.Context, params MergeEntriesParams) (MergeEntriesResult, error) {
					mu.Lock()
					defer mu.Unlock()
					if params.Refs[0].EntryKey == refs[0].EntryKey {
						failedCalls++
						return MergeEntriesResult{Failures: []MergeEntryFailure{{Ref: refs[0], Error: "still pending", NonRetryable: permanent}}}, nil
					}
					successfulCalls++
					return MergeEntriesResult{ResolvedCount: 1}, nil
				})
			env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{
				Key: Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100, BatchSize: 1,
			}})
			require.ErrorContains(t, env.GetWorkflowError(), "unresolved")
			encoded, err := env.QueryWorkflow(QueryTypeProgress)
			require.NoError(t, err)
			var progress ProgressQueryResponse
			require.NoError(t, encoded.Get(&progress))
			require.Equal(t, int64(1), progress.NumberOfMessagesProcessed)
			mu.Lock()
			defer mu.Unlock()
			expectedCalls := 10
			if permanent {
				expectedCalls = 1
			}
			require.Equal(t, expectedCalls, failedCalls)
			require.Equal(t, 1, successfulCalls)
		})
	}
}

func TestMixedMergeActivityErrorRetry(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
	env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{}, nil)
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(historydlq.EntryPage{Refs: refs}, nil)
	var mu sync.Mutex
	calls := 0
	env.OnActivity(mergeEntriesActivityName, mock.Anything, mock.Anything).Return(
		func(context.Context, MergeEntriesParams) (MergeEntriesResult, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				return MergeEntriesResult{}, serviceerror.NewUnavailable("activity response lost")
			}
			return MergeEntriesResult{ResolvedCount: 2}, nil
		})
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{
		Key: Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100,
	}})
	require.NoError(t, env.GetWorkflowError())
	encoded, err := env.QueryWorkflow(QueryTypeProgress)
	require.NoError(t, err)
	var progress ProgressQueryResponse
	require.NoError(t, encoded.Get(&progress))
	require.Equal(t, int64(2), progress.NumberOfMessagesProcessed)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, calls)
}

func TestMixedMergeCancellation(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
	env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{}, nil)
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(historydlq.EntryPage{Refs: refs, NextPageToken: []byte("next")}, nil).Once()
	env.OnActivity(mergeEntriesActivityName, mock.Anything, mock.Anything).Return(MergeEntriesResult{
		ResolvedCount: 1, Failures: []MergeEntryFailure{{Ref: refs[0], Error: "retry later"}},
	}, nil).Once()
	var progress ProgressQueryResponse
	var queryError error
	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(QueryTypeProgress)
		queryError = err
		if err == nil {
			queryError = encoded.Get(&progress)
		}
		env.CancelWorkflow()
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{
		Key: Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100,
	}})
	require.True(t, temporal.IsCanceledError(env.GetWorkflowError()))
	require.NoError(t, queryError)
	require.Equal(t, int64(1), progress.NumberOfMessagesProcessed)
	env.AssertExpectations(t)
}

func TestMixedPurgeProgress(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
	env.OnActivity(deleteTasksIfPresentActivityName, mock.Anything, mock.Anything).Return(&historyservice.DeleteDLQTasksResponse{MessagesDeleted: 2}, nil)
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.MatchedBy(func(req historydlq.ListEntriesRequest) bool {
		return len(req.NextPageToken) == 0
	})).Return(historydlq.EntryPage{Refs: refs[:1], NextPageToken: []byte("next")}, nil).Once()
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.MatchedBy(func(req historydlq.ListEntriesRequest) bool {
		return string(req.NextPageToken) == "next"
	})).Return(historydlq.EntryPage{Refs: refs[1:]}, nil).After(time.Second).Once()
	env.OnActivity(purgeEntriesActivityName, mock.Anything, PurgeEntriesParams{Segment: segment, EntryKeys: []string{refs[0].EntryKey}}).
		Return(PurgeEntriesResult{ResolvedEntryKeys: []string{refs[0].EntryKey}}, nil).Once()
	env.OnActivity(purgeEntriesActivityName, mock.Anything, PurgeEntriesParams{Segment: segment, EntryKeys: []string{refs[1].EntryKey}}).
		Return(PurgeEntriesResult{ResolvedEntryKeys: []string{refs[1].EntryKey}}, nil).Once()
	var during ProgressQueryResponse
	var queryError error
	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(QueryTypeProgress)
		queryError = err
		if err == nil {
			queryError = encoded.Get(&during)
		}
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeDelete, DeleteParams: DeleteParams{
		Key: Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100,
	}})
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, queryError)
	require.Equal(t, int64(3), during.NumberOfMessagesProcessed)
	encoded, err := env.QueryWorkflow(QueryTypeProgress)
	require.NoError(t, err)
	var final ProgressQueryResponse
	require.NoError(t, encoded.Get(&final))
	require.Equal(t, int64(4), final.NumberOfMessagesProcessed)
	require.Equal(t, int64(100), final.LastProcessedMessageID)
	env.AssertExpectations(t)
}

func TestMixedMergeKeepsLegacyDeletionBound(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	key := Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
	env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{
		DlqTasks: []*commonspb.HistoryDLQTask{{Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: 2}, Payload: &commonspb.HistoryTask{ShardId: 7}}},
	}, nil).Once()
	env.OnActivity(reEnqueueTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity(deleteTasksActivityName, mock.Anything, DeleteParams{Key: key, MaxMessageID: 2}).Return(&historyservice.DeleteDLQTasksResponse{MessagesDeleted: 1}, nil).Once()
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(historydlq.EntryPage{Refs: refs}, nil)
	env.OnActivity(mergeEntriesActivityName, mock.Anything, mock.Anything).Return(MergeEntriesResult{ResolvedCount: 2}, nil).Once()
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{Key: key, MaxMessageID: 100}})
	require.NoError(t, env.GetWorkflowError())
	encoded, err := env.QueryWorkflow(QueryTypeProgress)
	require.NoError(t, err)
	var progress ProgressQueryResponse
	require.NoError(t, encoded.Get(&progress))
	require.Equal(t, int64(3), progress.NumberOfMessagesProcessed)
	require.Equal(t, int64(99), progress.LastProcessedMessageID)
	env.AssertExpectations(t)
}

func TestMixedMergeActivityErrorExhaustion(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment}, nil)
	env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{}, nil)
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(historydlq.EntryPage{Refs: refs}, nil).Once()
	var mu sync.Mutex
	var attempts []int32
	env.OnActivity(mergeEntriesActivityName, mock.Anything, mock.Anything).Return(func(ctx context.Context, _ MergeEntriesParams) (MergeEntriesResult, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, activity.GetInfo(ctx).Attempt)
		return MergeEntriesResult{}, serviceerror.NewUnavailable("activity unavailable")
	})
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{
		Key: Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100,
	}})
	require.ErrorContains(t, env.GetWorkflowError(), "2 entries unresolved")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, attempts)
}

func TestMixedMergeContinuesOtherSegments(t *testing.T) {
	t.Parallel()
	segment, refs := mergeFixture(t)
	other := segment
	other.RunID = "other-run"
	otherRef := refs[1]
	otherRef.RunID = other.RunID
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component := &workerComponent{}
	component.RegisterWorkflow(env)
	component.RegisterActivities(env)
	env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{segment, other}, nil)
	env.OnActivity(readTasksActivityName, mock.Anything, mock.Anything, mock.Anything).Return(&historyservice.GetDLQTasksResponse{}, nil)
	env.OnActivity(listSegmentEntriesActivityName, mock.Anything, mock.Anything).Return(func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
		if req.Segment == segment {
			return historydlq.EntryPage{}, temporal.NewNonRetryableApplicationError("missing exact run", errorTypeInvalidRequest, nil)
		}
		return historydlq.EntryPage{Refs: []historydlq.MessageRef{otherRef}}, nil
	})
	env.OnActivity(mergeEntriesActivityName, mock.Anything, MergeEntriesParams{Segment: other, Refs: []historydlq.MessageRef{otherRef}}).
		Return(MergeEntriesResult{ResolvedCount: 1}, nil).Once()
	env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeMerge, MergeParams: MergeParams{
		Key: Key{TaskCategoryID: 3, SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: 100,
	}})
	require.ErrorContains(t, env.GetWorkflowError(), "missing exact run")
	encoded, err := env.QueryWorkflow(QueryTypeProgress)
	require.NoError(t, err)
	var progress ProgressQueryResponse
	require.NoError(t, encoded.Get(&progress))
	require.Equal(t, int64(1), progress.NumberOfMessagesProcessed)
	env.AssertExpectations(t)
}

func TestMixedPurgeLivePagesAndFreshCommand(t *testing.T) {
	t.Parallel()
	fixture := newPurgeFixture(t)
	for taskID := int64(10000); taskID < 10257; taskID++ {
		request := fixture.request
		request.SourceTaskIdentity.TaskID = taskID
		_, err := fixture.segment.Append(fixture.ctx, request)
		require.NoError(t, err)
	}
	initialCount := len(fixture.segment.Entries)
	for _, field := range fixture.segment.Entries {
		field.Get(fixture.ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
	}
	var mu sync.Mutex
	resolveCalls := 0
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testPurgeClient{
			segmentClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
				mu.Lock()
				defer mu.Unlock()
				return fixture.segment.ListEntries(fixture.ctx, req)
			}},
			resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
				mu.Lock()
				defer mu.Unlock()
				if err := fixture.segment.ResolveEntries(fixture.ctx, req); err != nil {
					return err
				}
				resolveCalls++
				if resolveCalls <= 2 {
					fixture.now = fixture.now.Add(time.Hour)
					request := fixture.request
					request.SourceTaskIdentity.TaskID = 20000 + int64(resolveCalls)
					_, err := fixture.segment.Append(fixture.ctx, request)
					return err
				}
				return nil
			},
		},
	}
	run := func() ProgressQueryResponse {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		component.RegisterWorkflow(env)
		component.RegisterActivities(env)
		env.OnActivity(discoverSegmentsActivityName, mock.Anything, mock.Anything).Return([]historydlq.SegmentRef{fixture.ref}, nil)
		env.OnActivity(deleteTasksIfPresentActivityName, mock.Anything, mock.Anything).Return(&historyservice.DeleteDLQTasksResponse{}, nil)
		env.ExecuteWorkflow(MixedWorkflowName, WorkflowParams{WorkflowType: WorkflowTypeDelete, DeleteParams: DeleteParams{
			Key: Key{TaskCategoryID: int(fixture.ref.Queue.CategoryID), SourceCluster: "source", TargetCluster: "target"}, MaxMessageID: math.MaxInt64,
		}})
		require.NoError(t, env.GetWorkflowError())
		encoded, err := env.QueryWorkflow(QueryTypeProgress)
		require.NoError(t, err)
		var progress ProgressQueryResponse
		require.NoError(t, encoded.Get(&progress))
		return progress
	}
	first := run()
	require.Equal(t, int64(initialCount+1), first.NumberOfMessagesProcessed)
	mu.Lock()
	require.Len(t, fixture.segment.Entries, 1)
	mu.Unlock()
	second := run()
	require.Equal(t, int64(1), second.NumberOfMessagesProcessed)
	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, fixture.segment.Entries)
	require.Equal(t, fixture.emptyBytes, fixture.segment.GetRetainedBytes())
}
