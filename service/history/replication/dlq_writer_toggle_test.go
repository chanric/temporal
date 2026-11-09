package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.uber.org/mock/gomock"
)

type recordV1Writer struct {
	requests []*persistence.PutReplicationTaskToDLQRequest
}

func (r *recordV1Writer) PutReplicationTaskToDLQ(
	_ context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	r.requests = append(r.requests, request)
	return nil
}

type recordCHASMWriter struct{ requests []historydlq.EnqueueRequest }

func (r *recordCHASMWriter) Enqueue(
	_ context.Context,
	request historydlq.EnqueueRequest,
) (historydlq.MessageRef, error) {
	r.requests = append(r.requests, request)
	return historydlq.MessageRef{MessageID: 42}, nil
}

func TestReplicationDLQWriterBackendSelection(t *testing.T) {
	t.Parallel()
	for _, v2 := range []bool{false, true} {
		for _, useCHASM := range []bool{false, true} {
			t.Run(fmt.Sprintf("v2=%v/chasm=%v", v2, useCHASM), func(t *testing.T) {
				t.Parallel()
				legacy := &recordV1Writer{}
				queue := &queuestest.FakeQueueWriter{}
				enqueuer := &recordCHASMWriter{}
				serializer := serialization.NewSerializer()
				ns := namespace.NewMockRegistry(gomock.NewController(t))
				ns.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
				shared := queues.NewDLQWriter(
					queues.NewDLQTaskEnqueuer(
						queue,
						enqueuer,
						serializer,
						func() bool { return useCHASM },
					),
					metrics.NoopMetricsHandler,
					log.NewTestLogger(),
					ns,
					chasm.NewRegistry(log.NewTestLogger()),
				)
				writer := newDLQWriterToggle(dlqWriterToggleParams{
					Config: &configs.Config{
						HistoryReplicationDLQV2: func() bool { return v2 },
					},
					ExecutionManagerDLQWriter: NewExecutionManagerDLQWriter(legacy),
					DLQWriterAdapter:          NewDLQWriterAdapter(shared, serializer, "target"),
				})
				req := DLQWriteRequest{
					SourceShardID: 13,
					SourceCluster: "source",
					TargetShardID: 26,
					ReplicationTaskInfo: &persistencespb.ReplicationTaskInfo{
						TaskType:    enumsspb.TASK_TYPE_REPLICATION_HISTORY,
						TaskId:      21,
						NamespaceId: "namespace",
						WorkflowId:  "workflow",
						RunId:       "run",
					},
				}
				require.NoError(t, writer.WriteTaskToDLQ(t.Context(), req))
				if !v2 {
					require.Len(t, legacy.requests, 1)
					require.Empty(t, queue.EnqueueTaskRequests)
					require.Empty(t, enqueuer.requests)
					return
				}
				require.Empty(t, legacy.requests)
				if !useCHASM {
					require.Len(t, queue.EnqueueTaskRequests, 1)
					require.Equal(t, 13, queue.EnqueueTaskRequests[0].SourceShardID)
					require.Empty(t, enqueuer.requests)
					return
				}
				require.Empty(t, queue.EnqueueTaskRequests)
				require.Len(t, enqueuer.requests, 1)
				got := enqueuer.requests[0]
				require.Equal(t, historydlq.TaskIdentity{
					SourceShardID: 13,
					TaskID:        21,
					NamespaceID:   "namespace",
					BusinessID:    "workflow",
					RunID:         "run",
				}, got.SourceTaskIdentity)
				require.Equal(t, historydlq.QueueKey{
					CategoryID:    3,
					SourceCluster: "source",
					TargetCluster: "target",
				}, got.Queue)
				require.Equal(t, int32(13), got.Task.ShardId)
			})
		}
	}
}
