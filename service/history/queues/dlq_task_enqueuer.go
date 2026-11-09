package queues

import (
	"context"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

// DLQTaskEnqueuer stores history tasks in a DLQ.
type DLQTaskEnqueuer interface {
	EnqueueTask(context.Context, *persistence.EnqueueTaskRequest) (int64, error)
}

type dlqTaskEnqueuerSelector struct {
	queueV2  DLQTaskEnqueuer
	chasm    DLQTaskEnqueuer
	useCHASM dynamicconfig.BoolPropertyFn
}

func NewDLQTaskEnqueuer(
	queueWriter QueueWriter,
	chasmEnqueuer CHASMEnqueuer,
	serializer serialization.Serializer,
	useCHASM dynamicconfig.BoolPropertyFn,
) DLQTaskEnqueuer {
	return &dlqTaskEnqueuerSelector{
		queueV2:  &queueV2TaskEnqueuer{queueWriter: queueWriter},
		chasm:    &chasmTaskEnqueuer{client: chasmEnqueuer, serializer: serializer},
		useCHASM: useCHASM,
	}
}

func (q *dlqTaskEnqueuerSelector) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (int64, error) {
	if q.useCHASM() {
		return q.chasm.EnqueueTask(ctx, request)
	}
	return q.queueV2.EnqueueTask(ctx, request)
}
