package queues_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type enqueueFunc func(context.Context, historydlq.EnqueueRequest) (historydlq.MessageRef, error)

func (f enqueueFunc) Enqueue(ctx context.Context, r historydlq.EnqueueRequest) (historydlq.MessageRef, error) {
	return f(ctx, r)
}

type forbiddenQueueWriter struct{ queues.QueueWriter }

func (forbiddenQueueWriter) CreateQueue(
	context.Context,
	*persistence.CreateQueueRequest,
) (*persistence.CreateQueueResponse, error) {
	panic("unexpected legacy queue write")
}
func (forbiddenQueueWriter) EnqueueTask(
	context.Context,
	*persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	panic("unexpected legacy enqueue")
}

func TestDLQWriterCHASM(t *testing.T) {
	t.Parallel()
	key := definition.NewWorkflowKey("namespace", "workflow", "run")
	for name, task := range map[string]tasks.Task{
		"workflow": &tasks.WorkflowTask{WorkflowKey: key, TaskID: 123},
		"CHASM": &tasks.ChasmTask{
			WorkflowKey: key,
			TaskID:      123,
			Category:    tasks.CategoryTransfer,
			Info: &persistencespb.ChasmTaskInfo{
				ArchetypeId: 42,
				Data: &commonpb.DataBlob{
					Data: []byte("data"),
				},
			},
		},
		"replication": &tasks.HistoryReplicationTask{WorkflowKey: key, TaskID: 123},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ns := namespace.NewMockRegistry(gomock.NewController(t))
			ns.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("namespace")).AnyTimes()
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			serializer := serialization.NewSerializer()
			var got historydlq.EnqueueRequest
			var choices int
			writer := queues.NewDLQWriter(
				queues.NewDLQTaskEnqueuer(
					forbiddenQueueWriter{},
					enqueueFunc(func(
						_ context.Context,
						req historydlq.EnqueueRequest,
					) (historydlq.MessageRef, error) {
						got = req
						return historydlq.MessageRef{MessageID: 77}, nil
					}),
					serializer,
					func() bool {
						choices++
						return choices == 1
					},
				),
				mh,
				log.NewTestLogger(),
				ns,
				chasm.NewRegistry(log.NewTestLogger()),
			)
			require.NoError(t, writer.WriteTaskToDLQ(t.Context(), "source", "target", 7, task, false))
			require.Equal(t, 1, choices)
			blob, err := serializer.SerializeTask(task)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &commonspb.HistoryTask{ShardId: 7, Blob: blob}, got.Task)
			got.Task = nil
			archetype := uint32(0)
			if typed, ok := task.(tasks.HasArchetypeID); ok {
				archetype = typed.GetArchetypeID()
			}
			require.Equal(t, historydlq.EnqueueRequest{
				Queue: historydlq.QueueKey{
					CategoryID:    int32(task.GetCategory().ID()),
					SourceCluster: "source",
					TargetCluster: "target",
				},
				SourceTaskIdentity: historydlq.TaskIdentity{
					SourceShardID: 7,
					TaskID:        123,
					NamespaceID:   "namespace",
					BusinessID:    "workflow",
					RunID:         "run",
					ArchetypeID:   archetype,
				},
			}, got)
			require.Len(t, capture.Snapshot()[metrics.DLQWrites.Name()], 1)
		})
	}
}

func TestDLQWriterCHASMFailure(t *testing.T) {
	t.Parallel()
	for _, rpcErr := range []error{
		serviceerror.NewUnavailable("ambiguous acceptance"),
		context.DeadlineExceeded,
		serviceerror.NewNotFound("accepted run unavailable"),
		serviceerror.NewInvalidArgument("transaction limit"),
		serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "overloaded"),
	} {
		t.Run(rpcErr.Error(), func(t *testing.T) {
			t.Parallel()
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			var attempts int
			writer := queues.NewDLQWriter(
				queues.NewDLQTaskEnqueuer(
					forbiddenQueueWriter{},
					enqueueFunc(func(
						context.Context,
						historydlq.EnqueueRequest,
					) (historydlq.MessageRef, error) {
						attempts++
						return historydlq.MessageRef{}, rpcErr
					}),
					serialization.NewSerializer(),
					func() bool { return true },
				),
				mh,
				log.NewTestLogger(),
				nil,
				chasm.NewRegistry(log.NewTestLogger()),
			)
			err := writer.WriteTaskToDLQ(
				t.Context(),
				"source",
				"target",
				7,
				&tasks.WorkflowTask{
					WorkflowKey: definition.NewWorkflowKey("namespace", "workflow", "run"),
					TaskID:      123,
				},
				true,
			)
			require.ErrorIs(t, err, queues.ErrSendTaskToDLQ)
			require.Equal(t, 1, attempts)
			require.Empty(t, capture.Snapshot()[metrics.DLQWrites.Name()])
			require.NotErrorAs(t, err, new(*serviceerror.NotFound))
		})
	}
}

func TestDLQWriterCHASMInvalidInput(t *testing.T) {
	t.Parallel()
	for _, shard := range []int{-1, 0, math.MaxInt32 + 1} {
		t.Run(fmt.Sprint(shard), func(t *testing.T) {
			t.Parallel()
			writer := queues.NewDLQWriter(
				queues.NewDLQTaskEnqueuer(
					forbiddenQueueWriter{},
					enqueueFunc(func(
						context.Context,
						historydlq.EnqueueRequest,
					) (historydlq.MessageRef, error) {
						panic("unexpected RPC")
					}),
					serialization.NewSerializer(),
					func() bool { return true },
				),
				metrics.NoopMetricsHandler,
				log.NewTestLogger(),
				nil,
				chasm.NewRegistry(log.NewTestLogger()),
			)
			require.ErrorIs(
				t,
				writer.WriteTaskToDLQ(
					t.Context(),
					"source",
					"target",
					shard,
					&tasks.WorkflowTask{
						TaskID: 123,
					},
					true,
				),
				queues.ErrSendTaskToDLQ,
			)
		})
	}
}

type failedTaskSerializer struct{ serialization.Serializer }

func (failedTaskSerializer) SerializeTask(tasks.Task) (*commonpb.DataBlob, error) {
	return nil, serviceerror.NewInternal("serialization failed")
}

func TestDLQWriterCHASMSerializationFailure(t *testing.T) {
	t.Parallel()
	writer := queues.NewDLQWriter(
		queues.NewDLQTaskEnqueuer(
			forbiddenQueueWriter{},
			enqueueFunc(func(
				context.Context,
				historydlq.EnqueueRequest,
			) (historydlq.MessageRef, error) {
				panic("unexpected RPC")
			}),
			failedTaskSerializer{},
			func() bool { return true },
		),
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
		nil,
		chasm.NewRegistry(log.NewTestLogger()),
	)
	require.ErrorIs(
		t,
		writer.WriteTaskToDLQ(
			t.Context(),
			"source",
			"target",
			7,
			&tasks.WorkflowTask{
				TaskID: 123,
			},
			true,
		),
		queues.ErrSendTaskToDLQ,
	)
}

func (s *executableSuite) TestCHASMDLQFailureRemainsRetryable() {
	s.testCHASMDLQFailureRemainsRetryable(serviceerror.NewNotFound("accepted run unavailable"))
}

func (s *executableSuite) TestCHASMDLQAdmissionRejectionRemainsPending() {
	s.testCHASMDLQFailureRemainsRetryable(serviceerror.NewResourceExhausted(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "segment storage budget exceeded",
	))
}

func (s *executableSuite) testCHASMDLQFailureRemainsRetryable(rpcErr error) {
	var attempts int
	var acceptedRequest historydlq.EnqueueRequest
	writer := queues.NewDLQWriter(
		queues.NewDLQTaskEnqueuer(
			forbiddenQueueWriter{},
			enqueueFunc(func(
				_ context.Context,
				req historydlq.EnqueueRequest,
			) (historydlq.MessageRef, error) {
				attempts++
				if attempts == 1 {
					acceptedRequest = req
					return historydlq.MessageRef{}, rpcErr
				}
				protorequire.ProtoEqual(s.T(), acceptedRequest.Task, req.Task)
				acceptedRequest.Task, req.Task = nil, nil
				s.Equal(acceptedRequest, req)
				return historydlq.MessageRef{MessageID: 77}, nil
			}),
			serialization.NewSerializer(),
			func() bool { return true },
		),
		s.metricsHandler,
		log.NewTestLogger(),
		s.mockNamespaceRegistry,
		s.chasmRegistry,
	)
	executable := s.newTestExecutable(func(p *params) {
		p.task = &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey("namespace", "workflow", "run"), TaskID: 123}
		p.dlqWriter = writer
		p.dlqEnabled = func() bool { return true }
		p.maxUnexpectedErrorAttempts = func() int { return 1 }
	})
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionErr: serviceerror.NewInternal("source failure"),
	}).Times(1)
	err := executable.Execute()
	s.Error(executable.HandleErr(err))
	err = executable.Execute()
	s.ErrorIs(err, queues.ErrSendTaskToDLQ)
	err = executable.HandleErr(err)
	s.Error(err)
	s.Equal(ctasks.TaskStatePending, executable.State())
	s.mockScheduler.EXPECT().TrySubmit(executable).Return(false)
	s.mockRescheduler.EXPECT().Add(executable, gomock.Any(), gomock.Any()).Do(func(
		_ queues.Executable, scheduledTime time.Time, _ queues.ThrottleKey,
	) {
		s.True(scheduledTime.After(s.timeSource.Now()))
	})
	executable.Nack(err)
	s.Equal(ctasks.TaskStatePending, executable.State())
	s.NoError(executable.Execute())
	s.Equal(2, attempts)
	executable.Ack()
	s.Equal(ctasks.TaskStateAcked, executable.State())
}
