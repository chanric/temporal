package tests

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/protobuf/encoding/protojson"
)

//parallelize:ignore // CLI apps mutate shared command and flag definitions.
func TestHistoryDLQQueueListing(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint("mixed=", mixed), func(t *testing.T) {
			job := newHistoryDLQJobTestEnv(t)
			manager, err := job.env.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
			require.NoError(t, err)
			t.Cleanup(manager.Close)
			legacyQueue := persistence.QueueKey{
				QueueType:     persistence.QueueTypeHistoryDLQ,
				Category:      tasks.CategoryTransfer,
				SourceCluster: job.key.SourceCluster,
				TargetCluster: job.key.TargetCluster,
			}
			var legacyCount int64
			if mixed {
				_, err := manager.CreateQueue(t.Context(), &persistence.CreateQueueRequest{QueueKey: legacyQueue})
				require.NoError(t, err)
				for i := range 2 {
					_, err := manager.EnqueueTask(t.Context(), &persistence.EnqueueTaskRequest{
						QueueType: persistence.QueueTypeHistoryDLQ, SourceCluster: job.key.SourceCluster, TargetCluster: job.key.TargetCluster,
						SourceShardID: int(job.producer.SourceShardID),
						Task:          &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey(job.producer.NamespaceID, job.producer.BusinessID, job.producer.RunID), TaskID: int64(200 + i)},
					})
					require.NoError(t, err)
				}
				legacyCount = 2
			}
			name := persistence.GetHistoryTaskQueueName(int(job.key.TaskCategory), job.key.SourceCluster, job.key.TargetCluster)
			want := &adminservice.ListQueuesResponse_QueueInfo{QueueName: name, MessageCount: legacyCount + 3, LastMessageId: job.bound}
			await.Require(t.Context(), t, func(t *await.T) {
				page, err := job.env.AdminClient().ListQueues(t.Context(), &adminservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 100})
				require.NoError(t, err)
				var actual *adminservice.ListQueuesResponse_QueueInfo
				for _, queue := range page.Queues {
					if queue.QueueName == name {
						actual = queue
					}
				}
				protorequire.ProtoEqual(t, want, actual)
			}, 15*time.Second, 50*time.Millisecond)
			legacyPage, err := job.env.GetTestCluster().Host().HistoryClient().ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 100})
			require.NoError(t, err)
			if mixed {
				protorequire.ProtoSliceEqual(t, []*historyservice.ListQueuesResponse_QueueInfo{
					{QueueName: name, MessageCount: 2, LastMessageId: 1},
				}, legacyPage.Queues)
			} else {
				require.Empty(t, legacyPage.Queues)
			}

			var output bytes.Buffer
			app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
				params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(job.env.FrontendGRPCAddress()))
				params.Writer = &output
			})
			require.NoError(t, app.RunContext(t.Context(), []string{
				"tdbg", "--" + tdbg.FlagYes, "dlq", "merge",
				"--" + tdbg.FlagDLQType, strconv.Itoa(int(job.key.TaskCategory)),
				"--" + tdbg.FlagCluster, job.key.SourceCluster, "--" + tdbg.FlagTargetCluster, job.key.TargetCluster,
			}))
			require.Contains(t, output.String(), fmt.Sprintf("Found last message ID: %d", job.bound))
			jsonStart := bytes.IndexByte(output.Bytes(), '{')
			require.NotEqual(t, -1, jsonStart)
			var response adminservice.MergeDLQTasksResponse
			require.NoError(t, protojson.Unmarshal(output.Bytes()[jsonStart:], &response))
			require.NoError(t, waitHistoryDLQJob(t, job.env, response.JobToken))
			job.requireProgress(t, response.JobToken, enumsspb.DLQ_OPERATION_STATE_COMPLETED, legacyCount+3)
			job.requireRemaining(t, nil)
			if mixed {
				legacyTasks, err := manager.ReadRawTasks(t.Context(), &persistence.ReadRawTasksRequest{
					QueueKey: legacyQueue, PageSize: 10,
				})
				require.NoError(t, err)
				require.Empty(t, legacyTasks.Tasks)
			}
			await.Require(t.Context(), t, func(t *await.T) {
				page, err := job.env.AdminClient().ListQueues(t.Context(), &adminservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 100})
				require.NoError(t, err)
				for _, queue := range page.Queues {
					if queue.QueueName == name {
						require.Zero(t, queue.MessageCount)
					}
				}
			}, 15*time.Second, 50*time.Millisecond)
		})
	}
}
