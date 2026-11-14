package tests

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
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

func TestHistoryDLQJobArrivalsDuringPaging(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{dlq.WorkflowTypeDelete, dlq.WorkflowTypeMerge} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			pageSize := historyDLQJobPageSize(operation)
			job := newHistoryDLQPagingJob(t, pageSize+1)
			nextRead, allowRead := make(chan struct{}), make(chan struct{})
			lastPage, allowLastPage := make(chan struct{}), make(chan struct{})
			releaseRead := sync.OnceFunc(func() { close(allowRead) })
			releaseLastPage := sync.OnceFunc(func() { close(allowLastPage) })
			var readPaused, lastPagePaused atomic.Bool
			generator := job.env.GetTestCluster().Host().GetGRPCFaultGenerator()
			t.Cleanup(generator.RegisterRequestCallback(grpcfaults.Scope{},
				func(ctx context.Context, _ string, request any) *grpcfaults.Outcome {
					req, ok := request.(*historydlqpb.ListEntriesRequest)
					if ok && req.GetSegment().GetRunId() == job.segment.RunID && len(req.NextPageToken) > 0 && readPaused.CompareAndSwap(false, true) {
						close(nextRead)
						select {
						case <-allowRead:
						case <-ctx.Done():
							return &grpcfaults.Outcome{Error: ctx.Err()}
						}
					}
					return nil
				}))
			t.Cleanup(generator.RegisterResponseCallback(grpcfaults.Scope{},
				func(ctx context.Context, _ string, request, response any, err error) *grpcfaults.Outcome {
					req, ok := request.(*historydlqpb.ListEntriesRequest)
					page, responseOK := response.(*historydlqpb.ListEntriesResponse)
					if ok && responseOK && req.GetSegment().GetRunId() == job.segment.RunID &&
						len(req.NextPageToken) > 0 && err == nil && len(page.NextPageToken) == 0 && lastPagePaused.CompareAndSwap(false, true) {
						close(lastPage)
						select {
						case <-allowLastPage:
						case <-ctx.Done():
							return &grpcfaults.Outcome{Error: ctx.Err()}
						}
					}
					return nil
				}))
			t.Cleanup(releaseRead)
			t.Cleanup(releaseLastPage)
			token := startHistoryDLQJob(t, job.env, operation, job.key, job.bound, 2)
			await.Rcv(t, nextRead)
			job.requireProgress(t, token, enumsspb.DLQ_OPERATION_STATE_RUNNING, int64(pageSize))
			job.requireRemaining(t, job.refs[pageSize:])
			arrival := job.enqueueTask(t, 100000)
			require.Greater(t, arrival.MessageID, job.refs[len(job.refs)-1].MessageID)
			releaseRead()
			await.Rcv(t, lastPage)
			lateArrival := job.enqueueTask(t, 100001)
			releaseLastPage()
			require.NoError(t, waitHistoryDLQJob(t, job.env, token))
			job.requireProgress(t, token, enumsspb.DLQ_OPERATION_STATE_COMPLETED, int64(pageSize+2))
			job.requireRemaining(t, []historydlq.MessageRef{lateArrival})
			stored, err := job.client.ReadMessage(t.Context(), lateArrival)
			require.NoError(t, err)
			require.NotNil(t, stored.Task)
			freshToken := runHistoryDLQJob(t, job.env, operation, job.key, job.bound, 2)
			job.requireProgress(t, freshToken, enumsspb.DLQ_OPERATION_STATE_COMPLETED, 1)
			job.requireRemaining(t, nil)
		})
	}
}

func TestHistoryDLQJobCancelDuringResolve(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{dlq.WorkflowTypeDelete, dlq.WorkflowTypeMerge} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			pageSize := historyDLQJobPageSize(operation)
			job := newHistoryDLQPagingJob(t, 2*pageSize+1)
			resolving, allowResolve := make(chan struct{}), make(chan struct{})
			resolved := make(chan error, 1)
			releaseResolve := sync.OnceFunc(func() { close(allowResolve) })
			var resolveCalls, continuationReads atomic.Int32
			var reportResolved sync.Once
			generator := job.env.GetTestCluster().Host().GetGRPCFaultGenerator()
			t.Cleanup(generator.RegisterRequestCallback(grpcfaults.Scope{},
				func(ctx context.Context, _ string, request any) *grpcfaults.Outcome {
					switch req := request.(type) {
					case *historydlqpb.ListEntriesRequest:
						if req.GetSegment().GetRunId() == job.segment.RunID && len(req.NextPageToken) > 0 {
							continuationReads.Add(1)
						}
					case *historydlqpb.ResolveEntriesRequest:
						if req.GetSegment().GetRunId() == job.segment.RunID && resolveCalls.Add(1) == 2 {
							close(resolving)
							select {
							case <-allowResolve:
							case <-ctx.Done():
								return &grpcfaults.Outcome{Error: ctx.Err()}
							}
						}
					default:
						return nil
					}
					return nil
				}))
			t.Cleanup(generator.RegisterResponseCallback(grpcfaults.Scope{},
				func(_ context.Context, _ string, request, _ any, err error) *grpcfaults.Outcome {
					if req, ok := request.(*historydlqpb.ResolveEntriesRequest); ok && req.GetSegment().GetRunId() == job.segment.RunID {
						for _, key := range req.EntryKeys {
							if key == job.refs[pageSize].EntryKey {
								reportResolved.Do(func() { resolved <- err })
							}
						}
					}
					return nil
				}))
			t.Cleanup(releaseResolve)
			token := startHistoryDLQJob(t, job.env, operation, job.key, job.bound, 2)
			await.Rcv(t, resolving)
			job.requireProgress(t, token, enumsspb.DLQ_OPERATION_STATE_RUNNING, int64(pageSize))
			job.requireRemaining(t, job.refs[pageSize:])
			canceled, err := job.env.AdminClient().CancelDLQJob(t.Context(), &adminservice.CancelDLQJobRequest{
				JobToken: token, Reason: "stop between DLQ pages",
			})
			require.NoError(t, err)
			require.True(t, canceled.Canceled)
			require.ErrorAs(t, waitHistoryDLQJob(t, job.env, token), new(*temporal.TerminatedError))
			job.requireProgress(t, token, enumsspb.DLQ_OPERATION_STATE_FAILED, int64(pageSize))
			releaseResolve()
			require.NoError(t, await.Rcv(t, resolved))
			job.env.CloseShard(primitives.SystemNamespaceID, job.segment.BusinessID)
			job.requireRemaining(t, job.refs[2*pageSize:])
			stored, err := job.client.ReadMessage(t.Context(), job.refs[2*pageSize])
			require.NoError(t, err)
			require.NotNil(t, stored.Task)
			require.Equal(t, int32(2), resolveCalls.Load())
			require.Equal(t, int32(1), continuationReads.Load())
			canceled, err = job.env.AdminClient().CancelDLQJob(t.Context(), &adminservice.CancelDLQJobRequest{JobToken: token})
			require.NoError(t, err)
			require.False(t, canceled.Canceled)
		})
	}
}

func historyDLQJobPageSize(operation string) int {
	if operation == dlq.WorkflowTypeDelete {
		return 256
	}
	return 2
}

func newHistoryDLQPagingJob(t *testing.T, count int) *historyDLQJobTestEnv {
	t.Helper()
	job := newHistoryDLQJobTestEnv(t)
	for i := len(job.refs); i < count; i++ {
		job.refs = append(job.refs, job.enqueueTask(t, int64(1000+i)))
	}
	// Put seeded entries before subsequent arrivals, independent of clock precision and hash ordering.
	for i := range job.refs {
		job.refs[i].MessageID = int64(1000 + i)
	}
	ctx, err := job.env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	_, _, err = chasm.UpdateComponent[*historydlq.Segment](ctx,
		chasm.NewComponentRef[*historydlq.Segment](chasm.ExecutionKey{
			NamespaceID: primitives.SystemNamespaceID, BusinessID: job.segment.BusinessID, RunID: job.segment.RunID,
		}), func(segment *historydlq.Segment, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			for _, ref := range job.refs {
				entry := segment.Entries[ref.EntryKey].Get(ctx)
				before := proto.Size(entry.EntryState)
				entry.MessageId = ref.MessageID
				segment.RetainedBytes += int64(proto.Size(entry.EntryState) - before)
			}
			segment.MinMessageId = job.refs[0].MessageID
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	job.bound = math.MaxInt64
	return job
}

func (j *historyDLQJobTestEnv) enqueueTask(t *testing.T, taskID int64) historydlq.MessageRef {
	t.Helper()
	producer := j.producer
	producer.TaskID = taskID
	task := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(producer.NamespaceID, producer.BusinessID, producer.RunID), TaskID: taskID,
	}
	blob, err := serialization.NewSerializer().SerializeTask(task)
	require.NoError(t, err)
	ref, err := j.client.Enqueue(t.Context(), historydlq.EnqueueRequest{
		Queue: j.segment.Queue, SourceTaskIdentity: producer, Task: &commonspb.HistoryTask{ShardId: producer.SourceShardID, Blob: blob},
	})
	require.NoError(t, err)
	return ref
}

func TestHistoryDLQJobDelayedVisibility(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{dlq.WorkflowTypeDelete, dlq.WorkflowTypeMerge} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t, testcore.WithDedicatedCluster(), testcore.WithHistoryShardCount(4),
				testcore.WithWorkerService("DLQ jobs with delayed visibility"),
				testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, false),
				testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, true),
				testcore.WithDynamicConfig(dynamicconfig.VisibilityProcessorSchedulerWorkerCount, 0),
			)
			tv := testvars.New(t)
			task := &tasks.WorkflowTask{
				WorkflowKey: definition.NewWorkflowKey(env.NamespaceID().String(), tv.WorkflowID(), tv.RunID()), TaskID: 123,
			}
			blob, err := serialization.NewSerializer().SerializeTask(task)
			require.NoError(t, err)
			client := historydlq.NewClient(env.Logger, env.GetTestCluster().Host().HistoryDLQClient())
			queue := historydlq.QueueKey{
				CategoryID: int32(tasks.CategoryTransfer.ID()), SourceCluster: cluster.TestCurrentClusterName, TargetCluster: cluster.TestCurrentClusterName,
			}
			sourceShardID := int32(tasks.GetShardIDForTask(task, 4))
			storedTask := &commonspb.HistoryTask{ShardId: sourceShardID, Blob: blob}
			ref, err := client.Enqueue(t.Context(), historydlq.EnqueueRequest{
				Queue: queue, SourceTaskIdentity: historydlq.TaskIdentity{
					SourceShardID: sourceShardID, TaskID: task.TaskID, NamespaceID: task.NamespaceID, BusinessID: task.WorkflowID, RunID: task.RunID,
				}, Task: storedTask,
			})
			require.NoError(t, err)
			env.CloseShard(primitives.SystemNamespaceID, ref.BusinessID)
			stored, err := client.ReadMessage(t.Context(), ref)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, storedTask, stored.Task)
			key := &commonspb.HistoryDLQKey{TaskCategory: queue.CategoryID, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster}
			hidden, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: key, PageSize: 10})
			require.NoError(t, err)
			require.Empty(t, hidden.DlqTasks)
			earlyToken := runHistoryDLQJob(t, env, operation, key, ref.MessageID, 1)
			earlyProgress, err := env.AdminClient().DescribeDLQJob(t.Context(), &adminservice.DescribeDLQJobRequest{JobToken: earlyToken})
			require.NoError(t, err)
			require.Equal(t, enumsspb.DLQ_OPERATION_STATE_COMPLETED, earlyProgress.OperationState)
			require.Zero(t, earlyProgress.MessagesProcessed)
			stored, err = client.ReadMessage(t.Context(), ref)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, storedTask, stored.Task)
			env.OverrideDynamicConfig(dynamicconfig.VisibilityProcessorSchedulerWorkerCount, 1)
			await.Require(t.Context(), t, func(t *await.T) {
				page, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: key, PageSize: 10})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &adminservice.GetDLQTasksResponse{DlqTasks: []*commonspb.HistoryDLQTask{{
					Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: ref.MessageID}, Payload: storedTask,
				}}}, page)
			}, 15*time.Second, 50*time.Millisecond)
			token := runHistoryDLQJob(t, env, operation, key, ref.MessageID, 1)
			progress, err := env.AdminClient().DescribeDLQJob(t.Context(), &adminservice.DescribeDLQJobRequest{JobToken: token})
			require.NoError(t, err)
			require.Equal(t, enumsspb.DLQ_OPERATION_STATE_COMPLETED, progress.OperationState)
			require.Equal(t, int64(1), progress.MessagesProcessed)
			_, err = client.ReadMessage(t.Context(), ref)
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
		})
	}
}
