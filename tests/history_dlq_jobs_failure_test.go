package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/dlq"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
)

func TestHistoryDLQMergePartialFailure(t *testing.T) {
	t.Parallel()
	for _, retryable := range []bool{true, false} {
		t.Run(fmt.Sprintf("retryable=%t", retryable), func(t *testing.T) {
			t.Parallel()
			var rejectedWorkflow atomic.Pointer[string]
			var failures atomic.Int32
			job := newHistoryDLQJobTestEnv(t, testcore.InjectPersistenceFault(func(target config.FaultInjectionTarget) error {
				request, ok := target.Request.(*persistence.InternalAddHistoryTasksRequest)
				rejected := rejectedWorkflow.Load()
				if !ok || rejected == nil || request.WorkflowID != *rejected {
					return nil
				}
				if retryable {
					if failures.CompareAndSwap(0, 1) {
						return &persistence.ConditionFailedError{Msg: "injected source write condition failed"}
					}
					return nil
				}
				failures.Add(1)
				return serviceerror.NewNotFound("injected source write target missing")
			}, testcore.WithStore(config.ExecutionStoreName), testcore.WithMethod("AddHistoryTasks")))
			rejectedWorkflow.Store(&job.rejectedWorkflowID)
			var partialResponse atomic.Bool
			t.Cleanup(job.env.GetTestCluster().Host().GetGRPCFaultGenerator().RegisterResponseCallback(grpcfaults.Scope{},
				func(_ context.Context, _ string, request, response any, err error) *grpcfaults.Outcome {
					req, ok := request.(*adminservice.AddTasksRequest)
					resp, responseOK := response.(*adminservice.AddTasksResponse)
					if ok && responseOK && err == nil && len(req.Tasks) == 3 && len(resp.Failures) == 2 {
						partialResponse.Store(true)
					}
					return nil
				}))
			response, err := job.env.AdminClient().MergeDLQTasks(t.Context(), &adminservice.MergeDLQTasksRequest{
				DlqKey: job.key, InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{MessageId: job.bound}, BatchSize: 3,
			})
			require.NoError(t, err)
			err = waitHistoryDLQJob(t, job.env, response.JobToken)
			require.True(t, partialResponse.Load(), "the real AddTasks batch must report only the two rejected entries")
			require.Equal(t, int32(1), failures.Load())
			if retryable {
				require.NoError(t, err)
				job.requireProgress(t, response.JobToken, enumsspb.DLQ_OPERATION_STATE_COMPLETED, 3)
				job.requireRemaining(t, nil)
				return
			}
			require.ErrorContains(t, err, "2 entries unresolved")
			job.requireProgress(t, response.JobToken, enumsspb.DLQ_OPERATION_STATE_FAILED, 1)
			job.env.CloseShard(primitives.SystemNamespaceID, job.segment.BusinessID)
			job.requireRemaining(t, job.refs[:2])
			for _, ref := range job.refs[:2] {
				stored, err := job.client.ReadMessage(t.Context(), ref)
				require.NoError(t, err)
				require.NotNil(t, stored.Task)
			}
			rejectedWorkflow.Store(nil)
			freshToken := runHistoryDLQJob(t, job.env, dlq.WorkflowTypeMerge, job.key, job.bound, 3)
			job.requireProgress(t, freshToken, enumsspb.DLQ_OPERATION_STATE_COMPLETED, 2)
			job.requireRemaining(t, nil)
		})
	}
}

func TestHistoryDLQJobLostResolveResponse(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{dlq.WorkflowTypeDelete, dlq.WorkflowTypeMerge} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			job := newHistoryDLQJobTestEnv(t)
			before := job.segmentState(t)
			var lost atomic.Bool
			var resolveCalls, submissionCalls atomic.Int32
			t.Cleanup(job.env.GetTestCluster().Host().GetGRPCFaultGenerator().RegisterResponseCallback(grpcfaults.Scope{},
				func(_ context.Context, _ string, request, _ any, err error) *grpcfaults.Outcome {
					if _, ok := request.(*adminservice.AddTasksRequest); ok {
						submissionCalls.Add(1)
					}
					if req, ok := request.(*historydlqpb.ResolveEntriesRequest); ok && req.GetSegment().GetRunId() == job.segment.RunID {
						resolveCalls.Add(1)
						if err == nil && lost.CompareAndSwap(false, true) {
							return &grpcfaults.Outcome{Error: serviceerror.NewUnavailable("lost successful resolve response")}
						}
					}
					return nil
				}))
			token := runHistoryDLQJob(t, job.env, operation, job.key, job.bound, 3)
			require.True(t, lost.Load())
			require.GreaterOrEqual(t, resolveCalls.Load(), int32(2))
			if operation == dlq.WorkflowTypeMerge {
				require.Equal(t, int32(1), submissionCalls.Load())
			} else {
				require.Zero(t, submissionCalls.Load())
			}
			job.requireProgress(t, token, enumsspb.DLQ_OPERATION_STATE_COMPLETED, 3)
			job.env.CloseShard(primitives.SystemNamespaceID, job.segment.BusinessID)
			job.requireRemaining(t, nil)
			after := job.segmentState(t)
			require.Equal(t, int64(-1), after.MinMessageId)
			require.Less(t, after.RetainedBytes, before.RetainedBytes)
			for _, ref := range job.refs {
				_, err := job.client.ReadMessage(t.Context(), ref)
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			}
		})
	}
}

func TestHistoryDLQPurgePersistenceFailure(t *testing.T) {
	t.Parallel()
	var failingRun atomic.Pointer[string]
	var failures atomic.Int32
	job := newHistoryDLQJobTestEnv(t, testcore.InjectPersistenceFault(func(target config.FaultInjectionTarget) error {
		request, ok := target.Request.(*persistence.InternalUpdateWorkflowExecutionRequest)
		runID := failingRun.Load()
		if ok && runID != nil && request.UpdateWorkflowMutation.RunID == *runID {
			failures.Add(1)
			return serviceerror.NewFailedPrecondition("injected segment write blocked")
		}
		return nil
	}, testcore.WithStore(config.ExecutionStoreName), testcore.WithMethod("UpdateWorkflowExecution")))
	before := job.segmentState(t)
	failedResponse, allowRetry := make(chan struct{}), make(chan struct{})
	notifyFailed := sync.OnceFunc(func() { close(failedResponse) })
	releaseRetry := sync.OnceFunc(func() { close(allowRetry) })
	var failed atomic.Bool
	generator := job.env.GetTestCluster().Host().GetGRPCFaultGenerator()
	t.Cleanup(generator.RegisterResponseCallback(grpcfaults.Scope{},
		func(_ context.Context, _ string, request, _ any, err error) *grpcfaults.Outcome {
			if req, ok := request.(*historydlqpb.ResolveEntriesRequest); ok && req.GetSegment().GetRunId() == job.segment.RunID && err != nil {
				failed.Store(true)
				notifyFailed()
			}
			return nil
		}))
	t.Cleanup(generator.RegisterRequestCallback(grpcfaults.Scope{},
		func(ctx context.Context, _ string, request any) *grpcfaults.Outcome {
			if req, ok := request.(*historydlqpb.ResolveEntriesRequest); ok && req.GetSegment().GetRunId() == job.segment.RunID && failed.Load() {
				select {
				case <-allowRetry:
				case <-ctx.Done():
					return &grpcfaults.Outcome{Error: ctx.Err()}
				}
			}
			return nil
		}))
	t.Cleanup(releaseRetry)
	failingRun.Store(&job.segment.RunID)
	response, err := job.env.AdminClient().PurgeDLQTasks(t.Context(), &adminservice.PurgeDLQTasksRequest{
		DlqKey: job.key, InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{MessageId: job.bound},
	})
	require.NoError(t, err)
	await.Rcv(t, failedResponse)
	failingRun.Store(nil)
	job.env.CloseShard(primitives.SystemNamespaceID, job.segment.BusinessID)
	protorequire.ProtoEqual(t, before, job.segmentState(t))
	job.requireRemaining(t, job.refs)
	for _, ref := range job.refs {
		stored, err := job.client.ReadMessage(t.Context(), ref)
		require.NoError(t, err)
		require.NotNil(t, stored.Task)
	}
	require.Equal(t, int32(1), failures.Load())
	releaseRetry()
	require.NoError(t, waitHistoryDLQJob(t, job.env, response.JobToken))
	job.requireProgress(t, response.JobToken, enumsspb.DLQ_OPERATION_STATE_COMPLETED, 3)
	job.env.CloseShard(primitives.SystemNamespaceID, job.segment.BusinessID)
	job.requireRemaining(t, nil)
	after := job.segmentState(t)
	require.Equal(t, int64(-1), after.MinMessageId)
	require.Less(t, after.RetainedBytes, before.RetainedBytes)
}

type historyDLQJobTestEnv struct {
	env                *testcore.TestEnv
	client             *historydlq.Client
	key                *commonspb.HistoryDLQKey
	segment            historydlq.SegmentRef
	refs               []historydlq.MessageRef
	bound              int64
	rejectedWorkflowID string
	producer           historydlq.TaskIdentity
}

func newHistoryDLQJobTestEnv(t *testing.T, opts ...testcore.TestOption) *historyDLQJobTestEnv {
	t.Helper()
	env := testcore.NewEnv(t, append([]testcore.TestOption{
		testcore.WithDedicatedCluster(), testcore.WithHistoryShardCount(4),
		testcore.WithWorkerService("history DLQ failure recovery"), testcore.WithDisableTestloggerFailure(),
		testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, false),
		testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, true),
	}, opts...)...)
	queue := historydlq.QueueKey{
		CategoryID:    int32(tasks.CategoryTransfer.ID()),
		SourceCluster: cluster.TestCurrentClusterName, TargetCluster: cluster.TestCurrentClusterName,
	}
	job := &historyDLQJobTestEnv{
		env: env, client: historydlq.NewClient(env.Logger, env.GetTestCluster().Host().HistoryDLQClient()),
		key: &commonspb.HistoryDLQKey{TaskCategory: queue.CategoryID, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster},
	}
	tv := testvars.New(t)
	var producers []historydlq.TaskIdentity
	for i := 0; i < 1000 && len(producers) < 2; i++ {
		task := &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey(
			env.NamespaceID().String(), fmt.Sprintf("%s-%d", tv.WorkflowID(), i), tv.RunID())}
		producer := historydlq.TaskIdentity{
			SourceShardID: int32(tasks.GetShardIDForTask(task, 4)), NamespaceID: task.NamespaceID,
			BusinessID: task.WorkflowID, RunID: task.RunID,
		}
		if len(producers) == 0 || producer.SourceShardID == producers[0].SourceShardID {
			producers = append(producers, producer)
		}
	}
	require.Len(t, producers, 2)
	job.producer = producers[0]
	job.rejectedWorkflowID = producers[0].BusinessID
	for i, producer := range []historydlq.TaskIdentity{producers[0], producers[0], producers[1]} {
		producer.TaskID = int64(100 + i)
		task := &tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(producer.NamespaceID, producer.BusinessID, producer.RunID), TaskID: producer.TaskID,
		}
		blob, err := serialization.NewSerializer().SerializeTask(task)
		require.NoError(t, err)
		ref, err := job.client.Enqueue(t.Context(), historydlq.EnqueueRequest{
			Queue: queue, SourceTaskIdentity: producer, Task: &commonspb.HistoryTask{ShardId: producer.SourceShardID, Blob: blob},
		})
		require.NoError(t, err)
		job.refs = append(job.refs, ref)
		job.bound = max(job.bound, ref.MessageID)
	}
	job.segment = historydlq.SegmentRef{Queue: queue, BusinessID: job.refs[0].BusinessID, RunID: job.refs[0].RunID}
	for _, ref := range job.refs {
		require.Equal(t, job.segment.BusinessID, ref.BusinessID)
		require.Equal(t, job.segment.RunID, ref.RunID)
	}
	await.Require(t.Context(), t, func(t *await.T) {
		page, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{
			DlqKey: job.key, InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{MessageId: job.bound}, PageSize: 3,
		})
		require.NoError(t, err)
		require.Len(t, page.DlqTasks, 3)
	}, 15*time.Second, 50*time.Millisecond)
	env.OverrideDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, false)
	return job
}

func (j *historyDLQJobTestEnv) requireRemaining(t *testing.T, refs []historydlq.MessageRef) {
	t.Helper()
	page, err := j.client.ListEntries(t.Context(), historydlq.ListEntriesRequest{
		Segment: j.segment, InclusiveMaxMessageID: j.bound, PageSize: 1000,
	})
	require.NoError(t, err)
	require.Empty(t, page.NextPageToken)
	require.ElementsMatch(t, refs, page.Refs)
}

func (j *historyDLQJobTestEnv) requireProgress(t *testing.T, token []byte, state enumsspb.DLQOperationState, count int64) {
	t.Helper()
	response, err := j.env.AdminClient().DescribeDLQJob(t.Context(), &adminservice.DescribeDLQJobRequest{JobToken: token})
	require.NoError(t, err)
	require.Equal(t, state, response.OperationState)
	require.Equal(t, count, response.MessagesProcessed)
}

func (j *historyDLQJobTestEnv) segmentState(t *testing.T) *historydlqpb.SegmentState {
	t.Helper()
	ctx, err := j.env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	state, err := chasm.ReadComponent[*historydlq.Segment](ctx,
		chasm.NewComponentRef[*historydlq.Segment](chasm.ExecutionKey{
			NamespaceID: primitives.SystemNamespaceID, BusinessID: j.segment.BusinessID, RunID: j.segment.RunID,
		}), func(segment *historydlq.Segment, _ chasm.Context, _ struct{}) (*historydlqpb.SegmentState, error) {
			return proto.Clone(segment.SegmentState).(*historydlqpb.SegmentState), nil
		}, struct{}{})
	require.NoError(t, err)
	return state
}
