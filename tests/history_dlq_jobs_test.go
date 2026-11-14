package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.temporal.io/server/service/worker/dlq"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestHistoryDLQJobs(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{dlq.WorkflowTypeDelete, dlq.WorkflowTypeMerge} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t, testcore.WithDedicatedCluster(), testcore.WithWorkerService("mixed history DLQ jobs"),
				testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, false),
				testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, true))
			tv := testvars.New(t)
			task := &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey(env.NamespaceID().String(), tv.WorkflowID(), tv.RunID()), TaskID: 123}
			blob, err := serialization.NewSerializer().SerializeTask(task)
			require.NoError(t, err)
			queue := historydlq.QueueKey{CategoryID: int32(tasks.CategoryTransfer.ID()), SourceCluster: cluster.TestCurrentClusterName, TargetCluster: cluster.TestCurrentClusterName}
			shardID := int32(tasks.GetShardIDForTask(task, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards)))
			client := historydlq.NewClient(env.Logger, env.GetTestCluster().Host().HistoryDLQClient())
			accepted, err := client.Enqueue(t.Context(), historydlq.EnqueueRequest{
				Queue: queue, SourceTaskIdentity: historydlq.TaskIdentity{
					SourceShardID: shardID, TaskID: 123, NamespaceID: task.NamespaceID, BusinessID: task.WorkflowID, RunID: task.RunID,
				}, Task: &commonspb.HistoryTask{ShardId: shardID, Blob: blob},
			})
			require.NoError(t, err)
			key := &commonspb.HistoryDLQKey{TaskCategory: queue.CategoryID, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster}
			bound := &commonspb.HistoryDLQTaskMetadata{MessageId: accepted.MessageID}
			await.Require(t.Context(), t, func(t *await.T) {
				page, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{
					DlqKey: key, InclusiveMaxTaskMetadata: bound, PageSize: 1,
				})
				require.NoError(t, err)
				require.Len(t, page.DlqTasks, 1)
			}, 15*time.Second, 50*time.Millisecond)
			env.OverrideDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, false)
			var jobToken []byte
			if operation == dlq.WorkflowTypeDelete {
				manager, err := env.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
				require.NoError(t, err)
				t.Cleanup(manager.Close)
				_, err = manager.CreateQueue(t.Context(), &persistence.CreateQueueRequest{QueueKey: persistence.QueueKey{
					QueueType: persistence.QueueTypeHistoryDLQ, Category: tasks.CategoryTransfer,
					SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster,
				}})
				require.NoError(t, err)
				empty, err := env.GetTestCluster().Host().HistoryClient().DeleteDLQTasks(t.Context(), &historyservice.DeleteDLQTasksRequest{
					DlqKey: key, InclusiveMaxTaskMetadata: bound,
				})
				require.NoError(t, err)
				require.Zero(t, empty.MessagesDeleted)
				response, err := env.AdminClient().PurgeDLQTasks(t.Context(), &adminservice.PurgeDLQTasksRequest{DlqKey: key, InclusiveMaxTaskMetadata: bound})
				require.NoError(t, err)
				jobToken = response.JobToken
			} else {
				response, err := env.AdminClient().MergeDLQTasks(t.Context(), &adminservice.MergeDLQTasksRequest{DlqKey: key, InclusiveMaxTaskMetadata: bound, BatchSize: 1})
				require.NoError(t, err)
				jobToken = response.JobToken
			}
			var token adminservice.DLQJobToken
			require.NoError(t, token.Unmarshal(jobToken))
			systemClient, err := sdkclient.Dial(sdkclient.Options{HostPort: env.FrontendGRPCAddress(), Namespace: primitives.SystemLocalNamespace})
			require.NoError(t, err)
			t.Cleanup(systemClient.Close)
			require.NoError(t, systemClient.GetWorkflow(t.Context(), token.WorkflowId, token.RunId).Get(t.Context(), nil))
			job, err := env.AdminClient().DescribeDLQJob(t.Context(), &adminservice.DescribeDLQJobRequest{JobToken: jobToken})
			require.NoError(t, err)
			require.Equal(t, enumsspb.DLQ_OPERATION_STATE_COMPLETED, job.OperationState)
			require.Equal(t, int64(1), job.MessagesProcessed)
			require.Equal(t, accepted.MessageID, job.MaxMessageId)
			stats, err := client.DescribeSegment(t.Context(), historydlq.SegmentRef{Queue: queue, BusinessID: accepted.BusinessID, RunID: accepted.RunID})
			require.NoError(t, err)
			require.Equal(t, historydlq.SegmentStats{}, stats)
		})
	}
}

func TestHistoryDLQJobsPages(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{dlq.WorkflowTypeDelete, dlq.WorkflowTypeMerge} {
		for _, backend := range []string{"queue-v2", "chasm", "writer-rollback-mixed"} {
			t.Run(operation+"/"+backend, func(t *testing.T) {
				t.Parallel()
				env := testcore.NewEnv(t, testcore.WithDedicatedCluster(), testcore.WithHistoryShardCount(4),
					testcore.WithWorkerService("paged history DLQ jobs"),
					testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, backend != "queue-v2"),
					testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, true))
				manager, err := env.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
				require.NoError(t, err)
				t.Cleanup(manager.Close)
				client := historydlq.NewClient(env.Logger, env.GetTestCluster().Host().HistoryDLQClient())
				config := dynamicconfig.NewCollection(env.GetTestCluster().Host().DcClient(), env.Logger)
				enqueuer := queues.NewDLQTaskEnqueuer(
					manager, client, serialization.NewSerializer(), dynamicconfig.HistoryTaskDLQUseCHASM.Get(config),
				)
				queue := historydlq.QueueKey{
					CategoryID:    int32(tasks.CategoryTransfer.ID()),
					SourceCluster: cluster.TestCurrentClusterName, TargetCluster: cluster.TestCurrentClusterName,
				}
				key := &commonspb.HistoryDLQKey{TaskCategory: queue.CategoryID, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster}
				bound := int64(2)
				var expectedCount int64
				var remaining []historydlq.MessageRef
				if backend != "queue-v2" {
					bound = 1 << 55
					entriesPerSegment := 3
					if operation == dlq.WorkflowTypeDelete {
						entriesPerSegment = 257 // Exceeds the purge metadata page size of 256.
					}
					remaining = seedHistoryDLQJobSegments(t, env, client, enqueuer, queue, entriesPerSegment, bound)
					expectedCount += int64(2 * entriesPerSegment)
					await.Require(t.Context(), t, func(t *await.T) {
						page, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{
							DlqKey: key, PageSize: 1000, InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{MessageId: bound},
						})
						require.NoError(t, err)
						require.Len(t, page.DlqTasks, 2*entriesPerSegment)
					}, 15*time.Second, 50*time.Millisecond)
				}
				env.OverrideDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, false)
				env.OverrideDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, false)
				var legacyRemaining []int64
				if backend != "chasm" {
					tv := testvars.New(t)
					for i := range 4 {
						task := &tasks.WorkflowTask{
							WorkflowKey: definition.NewWorkflowKey(env.NamespaceID().String(), tv.WorkflowID(), tv.RunID()),
							TaskID:      int64(10000 + i),
						}
						messageID, err := enqueuer.EnqueueTask(t.Context(), &persistence.EnqueueTaskRequest{
							QueueType: persistence.QueueTypeHistoryDLQ, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster,
							SourceShardID: tasks.GetShardIDForTask(task, 4), Task: task,
						})
						require.NoError(t, err)
						if messageID <= bound {
							expectedCount++
						} else {
							legacyRemaining = append(legacyRemaining, messageID)
						}
					}
				}
				jobToken := runHistoryDLQJob(t, env, operation, key, bound, 2)
				replayHistoryDLQJob(t, env, jobToken)
				job, err := env.AdminClient().DescribeDLQJob(t.Context(), &adminservice.DescribeDLQJobRequest{JobToken: jobToken})
				require.NoError(t, err)
				require.Equal(t, enumsspb.DLQ_OPERATION_STATE_COMPLETED, job.OperationState)
				require.Equal(t, expectedCount, job.MessagesProcessed)
				require.Equal(t, bound, job.MaxMessageId)
				for _, ref := range remaining {
					segment := historydlq.SegmentRef{Queue: queue, BusinessID: ref.BusinessID, RunID: ref.RunID}
					page, err := client.ListEntries(t.Context(), historydlq.ListEntriesRequest{
						Segment: segment, InclusiveMaxMessageID: bound + 1, PageSize: 1000,
					})
					require.NoError(t, err)
					require.Equal(t, historydlq.EntryPage{Refs: []historydlq.MessageRef{ref}}, page)
					stored, err := client.ReadMessage(t.Context(), ref)
					require.NoError(t, err)
					require.NotNil(t, stored.Task)
					stats, err := client.DescribeSegment(t.Context(), segment)
					require.NoError(t, err)
					require.Equal(t, historydlq.SegmentStats{PendingCount: 1, LastMessageID: bound + 1}, stats)
				}
				if backend != "chasm" {
					page, err := manager.ReadRawTasks(t.Context(), &persistence.ReadRawTasksRequest{
						QueueKey: persistence.QueueKey{QueueType: persistence.QueueTypeHistoryDLQ, Category: tasks.CategoryTransfer,
							SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster}, PageSize: 1000,
					})
					require.NoError(t, err)
					var ids []int64
					for _, task := range page.Tasks {
						ids = append(ids, task.MessageMetadata.ID)
					}
					require.Equal(t, legacyRemaining, ids)
				}
			})
		}
	}
}

func seedHistoryDLQJobSegments(
	t *testing.T,
	env *testcore.TestEnv,
	client *historydlq.Client,
	enqueuer queues.DLQTaskEnqueuer,
	queue historydlq.QueueKey,
	entriesPerSegment int,
	bound int64,
) []historydlq.MessageRef {
	t.Helper()
	tv := testvars.New(t)
	engineCtx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	var remaining []historydlq.MessageRef
	segments := make(map[string]bool)
	for producer := 0; producer < 100 && len(remaining) < 2; producer++ {
		task := &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey(
			env.NamespaceID().String(), fmt.Sprintf("%s-%d", tv.WorkflowID(), producer), tv.RunID())}
		shardID := int32(tasks.GetShardIDForTask(task, 4))
		identity := historydlq.TaskIdentity{
			SourceShardID: shardID, NamespaceID: task.NamespaceID,
			BusinessID: task.WorkflowID, RunID: task.RunID, TaskID: 1,
		}
		businessID, err := historydlq.SegmentBusinessID(queue, identity)
		require.NoError(t, err)
		if segments[businessID] {
			continue
		}
		segments[businessID] = true
		for i := range entriesPerSegment + 1 {
			task.TaskID = int64(1000*(producer+1) + i)
			_, err := enqueuer.EnqueueTask(t.Context(), &persistence.EnqueueTaskRequest{
				QueueType: persistence.QueueTypeHistoryDLQ, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster,
				SourceShardID: int(shardID), Task: task,
			})
			require.NoError(t, err)
		}
		identity.TaskID = task.TaskID
		blob, err := serialization.NewSerializer().SerializeTask(task)
		require.NoError(t, err)
		ref, err := client.Enqueue(t.Context(), historydlq.EnqueueRequest{
			Queue: queue, SourceTaskIdentity: identity, Task: &commonspb.HistoryTask{ShardId: shardID, Blob: blob},
		})
		require.NoError(t, err)
		// Force numeric ties across page and segment boundaries, with one entry above the bound.
		_, _, err = chasm.UpdateComponent[*historydlq.Segment](engineCtx,
			chasm.NewComponentRef[*historydlq.Segment](chasm.ExecutionKey{
				NamespaceID: primitives.SystemNamespaceID, BusinessID: ref.BusinessID, RunID: ref.RunID,
			}), func(segment *historydlq.Segment, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				for key, field := range segment.Entries {
					entry := field.Get(ctx)
					entry.MessageId = bound
					if key == ref.EntryKey {
						entry.MessageId = bound + 1
					}
				}
				segment.MinMessageId = bound
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
		ref.MessageID = bound + 1
		remaining = append(remaining, ref)
	}
	require.Len(t, remaining, 2)
	return remaining
}

func runHistoryDLQJob(
	t *testing.T,
	env *testcore.TestEnv,
	operation string,
	key *commonspb.HistoryDLQKey,
	bound int64,
	batchSize int32,
) []byte {
	t.Helper()
	jobToken := startHistoryDLQJob(t, env, operation, key, bound, batchSize)
	require.NoError(t, waitHistoryDLQJob(t, env, jobToken))
	return jobToken
}

func startHistoryDLQJob(
	t *testing.T,
	env *testcore.TestEnv,
	operation string,
	key *commonspb.HistoryDLQKey,
	bound int64,
	batchSize int32,
) []byte {
	t.Helper()
	metadata := &commonspb.HistoryDLQTaskMetadata{MessageId: bound}
	var jobToken []byte
	if operation == dlq.WorkflowTypeDelete {
		response, err := env.AdminClient().PurgeDLQTasks(t.Context(), &adminservice.PurgeDLQTasksRequest{
			DlqKey: key, InclusiveMaxTaskMetadata: metadata,
		})
		require.NoError(t, err)
		jobToken = response.JobToken
	} else {
		response, err := env.AdminClient().MergeDLQTasks(t.Context(), &adminservice.MergeDLQTasksRequest{
			DlqKey: key, InclusiveMaxTaskMetadata: metadata, BatchSize: batchSize,
		})
		require.NoError(t, err)
		jobToken = response.JobToken
	}
	return jobToken
}

func waitHistoryDLQJob(t *testing.T, env *testcore.TestEnv, jobToken []byte) error {
	t.Helper()
	var token adminservice.DLQJobToken
	require.NoError(t, token.Unmarshal(jobToken))
	systemClient, err := sdkclient.Dial(sdkclient.Options{HostPort: env.FrontendGRPCAddress(), Namespace: primitives.SystemLocalNamespace})
	require.NoError(t, err)
	t.Cleanup(systemClient.Close)
	return systemClient.GetWorkflow(t.Context(), token.WorkflowId, token.RunId).Get(t.Context(), nil)
}

type historyDLQReplayRegistry struct {
	sdkworker.Registry
	replayer sdkworker.WorkflowReplayer
}

func (r historyDLQReplayRegistry) RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions) {
	r.replayer.RegisterWorkflowWithOptions(w, options)
}

func replayHistoryDLQJob(t *testing.T, env *testcore.TestEnv, jobToken []byte) {
	t.Helper()
	var token adminservice.DLQJobToken
	require.NoError(t, token.Unmarshal(jobToken))
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       primitives.SystemLocalNamespace,
		Execution:       &commonpb.WorkflowExecution{WorkflowId: token.WorkflowId, RunId: token.RunId},
		MaximumPageSize: 1000,
	}
	history := &historypb.History{}
	for {
		page, err := env.FrontendClient().GetWorkflowExecutionHistory(t.Context(), request)
		require.NoError(t, err)
		history.Events = append(history.Events, page.History.Events...)
		if len(page.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = page.NextPageToken
	}
	var components []workercommon.WorkerComponent
	// Replay uses recorded activity results.
	fxtest.New(t, dlq.Module,
		fx.Supply(historydlq.NewClient(env.Logger, nil), dlq.CurrentClusterName(cluster.TestCurrentClusterName)),
		fx.Provide(
			func() dlq.HistoryClient { return nil },
			func() dlq.TaskClientDialer { return nil },
			func() chasm.VisibilityManager { return nil },
		),
		fx.Populate(fx.Annotate(&components, fx.ParamTags(workercommon.WorkerComponentTag))),
	)
	require.Len(t, components, 1)
	replayer := sdkworker.NewWorkflowReplayer()
	components[0].RegisterWorkflow(historyDLQReplayRegistry{replayer: replayer})
	require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(env.Logger), history))
	t.Logf("Replayed DLQ job history with %d events", len(history.Events))
}
