package queues_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
)

func TestDLQTaskEnqueuerSwitchesBackend(t *testing.T) {
	t.Parallel()
	queueWriter := &queuestest.FakeQueueWriter{
		EnqueueTaskFunc: func(context.Context, *persistence.EnqueueTaskRequest) (*persistence.EnqueueTaskResponse, error) {
			return &persistence.EnqueueTaskResponse{Metadata: persistence.MessageMetadata{ID: 11}}, nil
		},
	}
	var chasmAttempts, choices int
	var useCHASM bool
	enqueuer := queues.NewDLQTaskEnqueuer(
		queueWriter,
		enqueueFunc(func(context.Context, historydlq.EnqueueRequest) (historydlq.MessageRef, error) {
			chasmAttempts++
			return historydlq.MessageRef{MessageID: 77}, nil
		}),
		serialization.NewSerializer(),
		func() bool {
			choices++
			return useCHASM
		},
	)
	request := &persistence.EnqueueTaskRequest{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		SourceCluster: "source",
		TargetCluster: "target",
		SourceShardID: 7,
		Task:          &tasks.WorkflowTask{TaskID: 123},
	}
	for _, tc := range []struct {
		useCHASM  bool
		messageID int64
	}{
		{false, 11},
		{true, 77},
		{false, 11},
	} {
		useCHASM = tc.useCHASM
		messageID, err := enqueuer.EnqueueTask(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, tc.messageID, messageID)
	}
	require.Equal(t, 3, choices)
	require.Equal(t, 1, chasmAttempts)
	require.Equal(t, []*persistence.EnqueueTaskRequest{request, request}, queueWriter.EnqueueTaskRequests)
}
