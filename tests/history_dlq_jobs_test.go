package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/dlq"
	"go.temporal.io/server/tests/testcore"
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
