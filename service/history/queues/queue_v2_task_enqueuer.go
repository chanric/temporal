package queues

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.temporal.io/server/common/persistence"
)

type (
	// QueueWriter is a subset of persistence.HistoryTaskQueueManager.
	QueueWriter interface {
		CreateQueue(
			ctx context.Context,
			request *persistence.CreateQueueRequest,
		) (*persistence.CreateQueueResponse, error)
		EnqueueTask(
			ctx context.Context,
			request *persistence.EnqueueTaskRequest,
		) (*persistence.EnqueueTaskResponse, error)
	}
	queueV2TaskEnqueuer struct {
		queueWriter  QueueWriter
		enqueueMutex sync.Map // map[persistence.QueueKey]*sync.Mutex for per-queue locking
	}
)

func (q *queueV2TaskEnqueuer) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (int64, error) {
	queueKey := persistence.QueueKey{
		QueueType:     request.QueueType,
		Category:      request.Task.GetCategory(),
		SourceCluster: request.SourceCluster,
		TargetCluster: request.TargetCluster,
	}
	_, err := q.queueWriter.CreateQueue(ctx, &persistence.CreateQueueRequest{QueueKey: queueKey})
	if err != nil && !errors.Is(err, persistence.ErrQueueAlreadyExists) {
		return 0, fmt.Errorf("%w: %v", ErrCreateDLQ, err)
	}
	mu := q.getQueueMutex(queueKey)
	mu.Lock()
	defer mu.Unlock()
	response, err := q.queueWriter.EnqueueTask(ctx, request)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	return response.Metadata.ID, nil
}

// getQueueMutex returns a per-queue mutex, creating it if it doesn't exist.
// This provides process-level locking to serialize concurrent writes to the same queue.
func (q *queueV2TaskEnqueuer) getQueueMutex(queueKey persistence.QueueKey) *sync.Mutex {
	if mu, ok := q.enqueueMutex.Load(queueKey); ok {
		return mu.(*sync.Mutex) //nolint:revive
	}

	newMutex := &sync.Mutex{}
	actual, _ := q.enqueueMutex.LoadOrStore(queueKey, newMutex)
	return actual.(*sync.Mutex) //nolint:revive
}
