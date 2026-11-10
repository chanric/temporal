package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
)

func TestAddTasksBatchFailures(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	tv := testvars.New(t)
	task := &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey(env.NamespaceID().String(), tv.WorkflowID(), tv.RunID())}
	blob, err := serialization.NewSerializer().SerializeTask(task)
	require.NoError(t, err)
	request := &adminservice.AddTasksRequest{
		ShardId:            int32(tasks.GetShardIDForTask(task, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards))),
		ReturnTaskFailures: true,
		Tasks: []*adminservice.AddTasksRequest_Task{
			{CategoryId: int32(tasks.CategoryTransfer.ID())},
			{CategoryId: int32(tasks.CategoryTransfer.ID()), Blob: blob},
		},
	}
	response, err := env.AdminClient().AddTasks(t.Context(), request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &adminservice.AddTasksResponse{Failures: []*commonspb.AddTaskFailure{
		{TaskIndex: 0, Message: "Task blob is nil at index: 0", NonRetryable: true},
	}}, response)
	request.ReturnTaskFailures = false
	_, err = env.AdminClient().AddTasks(t.Context(), request)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
}
