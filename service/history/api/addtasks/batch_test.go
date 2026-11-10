package addtasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api/addtasks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func batchTask(t *testing.T, workflowID string) *historyservice.AddTasksRequest_Task {
	t.Helper()
	blob, err := serialization.NewSerializer().SerializeTask(&tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(string(tests.NamespaceID), workflowID, "run"),
	})
	require.NoError(t, err)
	return &historyservice.AddTasksRequest_Task{CategoryId: int32(tasks.CategoryTransfer.ID()), Blob: blob}
}

func TestReturnTaskFailures(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"nil task", "nil blob", "unknown category", "invalid blob"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			params := getDefaultTestParams(t)
			bad := batchTask(t, "bad")
			switch scenario {
			case "nil task":
				bad = nil
			case "nil blob":
				bad.Blob = nil
			case "unknown category":
				bad.CategoryId = -1
			case "invalid blob":
				bad.Blob.Data = []byte("invalid task")
			default:
				t.Fatalf("unknown scenario: %s", scenario)
			}
			params.req.Tasks = []*historyservice.AddTasksRequest_Task{bad, batchTask(t, "good")}
			params.req.ReturnTaskFailures = true
			params.shardContext.(*historyi.MockShardContext).EXPECT().AddTasks(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *persistence.AddHistoryTasksRequest) error {
					require.Equal(t, "good", req.WorkflowID)
					return nil
				})
			response, err := addtasks.Invoke(t.Context(), params.shardContext, params.deserializer,
				params.numShards, params.req, tasks.NewDefaultTaskCategoryRegistry())
			require.NoError(t, err)
			require.Len(t, response.Failures, 1)
			require.Zero(t, response.Failures[0].TaskIndex)
			require.NotEmpty(t, response.Failures[0].Message)
			if scenario != "invalid blob" {
				require.True(t, response.Failures[0].NonRetryable)
			}
		})
	}
}

func TestReturnGroupFailures(t *testing.T) {
	t.Parallel()
	params := getDefaultTestParams(t)
	params.req.ReturnTaskFailures = true
	params.req.Tasks = []*historyservice.AddTasksRequest_Task{batchTask(t, "uncertain"), batchTask(t, "good"), batchTask(t, "uncertain")}
	var workflows []string
	params.shardContext.(*historyi.MockShardContext).EXPECT().AddTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *persistence.AddHistoryTasksRequest) error {
			workflows = append(workflows, req.WorkflowID)
			if req.WorkflowID == "uncertain" {
				require.Len(t, req.Tasks[tasks.CategoryTransfer], 2)
				return serviceerror.NewUnavailable("persistence response lost")
			}
			return nil
		}).Times(2)
	response, err := addtasks.Invoke(t.Context(), params.shardContext, params.deserializer,
		params.numShards, params.req, tasks.NewDefaultTaskCategoryRegistry())
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &historyservice.AddTasksResponse{Failures: []*commonspb.AddTaskFailure{
		{TaskIndex: 0, Message: "persistence response lost"}, {TaskIndex: 2, Message: "persistence response lost"},
	}}, response)
	require.ElementsMatch(t, []string{"uncertain", "good"}, workflows)
}

func TestReturnTaskFailuresRequestLimits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		count int
	}{
		{name: "empty request", count: 0}, {name: "too many tasks", count: 1001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			params := getDefaultTestParams(t)
			params.req.ReturnTaskFailures = true
			params.req.Tasks = make([]*historyservice.AddTasksRequest_Task, tc.count)
			_, err := addtasks.Invoke(t.Context(), params.shardContext, params.deserializer,
				params.numShards, params.req, tasks.NewDefaultTaskCategoryRegistry())
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}
