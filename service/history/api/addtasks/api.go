package addtasks

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc/codes"
)

type (
	// TaskDeserializer is a trimmed version of [go.temporal.io/server/common/persistence/serialization.Serializer] that
	// requires only the DeserializeTask method.
	TaskDeserializer interface {
		DeserializeTask(category tasks.Category, blob *commonpb.DataBlob) (tasks.Task, error)
	}

	taskGroup struct {
		tasks   map[tasks.Category][]tasks.Task
		indices []int
	}
	taskGroupKey struct {
		namespaceID string
		businessID  string
		archetypeID chasm.ArchetypeID
	}
)

const (
	// maxTasksPerRequest is the maximum number of tasks that can be added in a single AddTasks API call. We set this to
	// prevent the history service from OOMing when a client sends a request with a large number of tasks because we
	// will deserialize all tasks in memory before adding them to the queue.
	maxTasksPerRequest = 1000
)

// Invoke groups tasks by namespace, business ID and archetype before persisting them.
// When requested, it reports failed task indexes and continues independent groups.
func Invoke(
	ctx context.Context,
	shardContext historyi.ShardContext,
	deserializer TaskDeserializer,
	numShards int,
	req *historyservice.AddTasksRequest,
	taskRegistry tasks.TaskCategoryRegistry,
) (*historyservice.AddTasksResponse, error) {
	if len(req.Tasks) > maxTasksPerRequest {
		return nil, serviceerror.NewInvalidArgumentf("Too many tasks in request: %d > %d", len(req.Tasks), maxTasksPerRequest)
	}
	if len(req.Tasks) == 0 {
		return nil, serviceerror.NewInvalidArgument("No tasks in request")
	}
	taskGroups := make(map[taskGroupKey]*taskGroup)
	failures := make([]*commonspb.AddTaskFailure, len(req.Tasks))
	for i := range req.Tasks {
		category, task, err := deserializeTask(req, i, deserializer, numShards, taskRegistry)
		if err != nil {
			if !req.ReturnTaskFailures {
				return nil, err
			}
			failures[i] = taskFailure(i, err)
			continue
		}
		archetypeID := chasm.WorkflowArchetypeID
		if hasArchetypeID, ok := task.(tasks.HasArchetypeID); ok {
			archetypeID = hasArchetypeID.GetArchetypeID()
		}
		if archetypeID == chasm.UnspecifiedArchetypeID {
			archetypeID = chasm.WorkflowArchetypeID
		}
		key := taskGroupKey{namespaceID: task.GetNamespaceID(), businessID: task.GetWorkflowID(), archetypeID: archetypeID}
		group, present := taskGroups[key]
		if !present {
			group = &taskGroup{tasks: make(map[tasks.Category][]tasks.Task)}
			taskGroups[key] = group
		}
		group.tasks[category] = append(group.tasks[category], task)
		group.indices = append(group.indices, i)
	}
	for key, group := range taskGroups {
		err := shardContext.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
			ShardID: shardContext.GetShardID(), RangeID: shardContext.GetRangeID(),
			NamespaceID: key.namespaceID, WorkflowID: key.businessID, ArchetypeID: key.archetypeID, Tasks: group.tasks,
		})
		if err != nil {
			if !req.ReturnTaskFailures {
				return nil, err
			}
			for _, i := range group.indices {
				failures[i] = taskFailure(i, err)
			}
		}
	}
	response := &historyservice.AddTasksResponse{}
	for _, failure := range failures {
		if failure != nil {
			response.Failures = append(response.Failures, failure)
		}
	}
	return response, nil
}

func deserializeTask(
	req *historyservice.AddTasksRequest, index int, deserializer TaskDeserializer,
	numShards int, registry tasks.TaskCategoryRegistry,
) (tasks.Category, tasks.Task, error) {
	task := req.Tasks[index]
	if task == nil {
		return tasks.Category{}, nil, serviceerror.NewInvalidArgumentf("Nil task at index: %d", index)
	}
	category, err := api.GetTaskCategory(int(task.CategoryId), registry)
	if err != nil {
		return tasks.Category{}, nil, err
	}
	if task.Blob == nil {
		return tasks.Category{}, nil, serviceerror.NewInvalidArgumentf("Task blob is nil at index: %d", index)
	}
	deserialized, err := deserializer.DeserializeTask(category, task.Blob)
	if err != nil {
		return tasks.Category{}, nil, err
	}
	shardID := tasks.GetShardIDForTask(deserialized, numShards)
	if shardID != int(req.ShardId) {
		return tasks.Category{}, nil, serviceerror.NewInvalidArgumentf(
			"Task is for wrong shard: index = %d, task shard = %d, request shard = %d", index, shardID, req.ShardId,
		)
	}
	return category, deserialized, nil
}

func taskFailure(index int, err error) *commonspb.AddTaskFailure {
	code := serviceerror.ToStatus(err).Code()
	return &commonspb.AddTaskFailure{
		TaskIndex: int32(index), Message: err.Error(), NonRetryable: code == codes.InvalidArgument || code == codes.NotFound,
	}
}
