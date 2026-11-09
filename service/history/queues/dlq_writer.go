package queues

import (
	"context"
	"errors"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

// DLQWriter can be used to write tasks to the DLQ.
type DLQWriter struct {
	taskEnqueuer      DLQTaskEnqueuer
	metricsHandler    metrics.Handler
	logger            log.SnTaggedLogger
	namespaceRegistry namespace.Registry
	chasmRegistry     *chasm.Registry
}

var (
	ErrSendTaskToDLQ = errors.New("failed to send task to DLQ")
	ErrCreateDLQ     = errors.New("failed to create DLQ")
)

// NewDLQWriter returns a DLQ writer using the given task enqueuer.
func NewDLQWriter(
	taskEnqueuer DLQTaskEnqueuer,
	metricsHandler metrics.Handler,
	logger log.SnTaggedLogger,
	namespaceRegistry namespace.Registry,
	chasmRegistry *chasm.Registry,
) *DLQWriter {
	return &DLQWriter{
		taskEnqueuer:      taskEnqueuer,
		metricsHandler:    metricsHandler,
		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		chasmRegistry:     chasmRegistry,
	}
}

// WriteTaskToDLQ writes a task to the DLQ and records the successful write.
func (q *DLQWriter) WriteTaskToDLQ(
	ctx context.Context,
	sourceCluster, targetCluster string,
	sourceShardID int,
	task tasks.Task,
	isNamespaceActive bool,
) error {
	messageID, err := q.taskEnqueuer.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
		SourceShardID: sourceShardID,
		Task:          task,
	})
	if err != nil {
		return err
	}

	nsMetricTag := metrics.NamespaceUnknownTag()
	var nsLogTag tag.Tag
	ns, err := q.namespaceRegistry.GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
	if err != nil {
		q.logger.Warn("Failed to get namespace name while trying to write a task to DLQ",
			tag.WorkflowNamespace(task.GetNamespaceID()),
			tag.Error(err),
		)
		nsLogTag = tag.WorkflowNamespaceID(task.GetNamespaceID())
	} else {
		nsMetricTag = metrics.NamespaceTag(ns.Name().String())
		nsLogTag = tag.WorkflowNamespace(ns.Name().String())
	}

	// "passive" means the namespace is in standby mode and only replicates data
	namespaceState := metrics.PassiveNamespaceStateTagValue
	if isNamespaceActive {
		namespaceState = metrics.ActiveNamespaceStateTagValue
	}
	taskType := GetTaskTypeTagValue(task, isNamespaceActive, q.chasmRegistry)
	metrics.DLQWrites.With(q.metricsHandler).Record(
		1,
		metrics.TaskCategoryTag(task.GetCategory().Name()),
		metrics.NamespaceStateTag(namespaceState),
		nsMetricTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType),
		getArchetypeTag(task, q.chasmRegistry),
	)
	q.logger.Warn("Task enqueued to DLQ",
		tag.DLQMessageID(messageID),
		tag.SourceCluster(sourceCluster),
		tag.TargetCluster(targetCluster),
		tag.TaskType(task.GetType()),
		tag.String("task-category", task.GetCategory().Name()),
		nsLogTag,
	)
	return nil
}
