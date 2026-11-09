package queues

import (
	"context"
	"fmt"
	"math"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

type CHASMEnqueuer interface {
	Enqueue(context.Context, historydlq.EnqueueRequest) (historydlq.MessageRef, error)
}

type chasmTaskEnqueuer struct {
	client     CHASMEnqueuer
	serializer serialization.Serializer
}

func (q *chasmTaskEnqueuer) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (int64, error) {
	sourceShardID := request.SourceShardID
	task := request.Task
	if sourceShardID <= 0 || sourceShardID > math.MaxInt32 {
		return 0, fmt.Errorf("%w: invalid source shard %d", ErrSendTaskToDLQ, sourceShardID)
	}
	blob, err := q.serializer.SerializeTask(task)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	sourceTaskIdentity := historydlq.TaskIdentity{
		SourceShardID: int32(sourceShardID),
		TaskID:        task.GetTaskID(),
		NamespaceID:   task.GetNamespaceID(),
		BusinessID:    task.GetWorkflowID(),
		RunID:         task.GetRunID(),
	}
	if archetypeTask, ok := task.(tasks.HasArchetypeID); ok {
		sourceTaskIdentity.ArchetypeID = archetypeTask.GetArchetypeID()
	}
	ref, err := q.client.Enqueue(ctx, historydlq.EnqueueRequest{
		Queue: historydlq.QueueKey{
			CategoryID:    int32(task.GetCategory().ID()),
			SourceCluster: request.SourceCluster,
			TargetCluster: request.TargetCluster,
		},
		SourceTaskIdentity: sourceTaskIdentity,
		Task:               &commonspb.HistoryTask{ShardId: int32(sourceShardID), Blob: blob},
	})
	if err != nil {
		// Keep storage failures inside the DLQ write retry boundary.
		return 0, fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	return ref.MessageID, nil
}
