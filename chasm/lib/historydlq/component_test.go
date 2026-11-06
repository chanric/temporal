package historydlq

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func componentRequest() EnqueueRequest {
	req := testRequest()
	req.Task = &commonspb.HistoryTask{ShardId: 7, Blob: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("task payload")}}
	return req
}

func TestMessagePersistence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 4, 12, 0, 0, 123, time.UTC)
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	backend := &chasm.MockNodeBackend{
		HandleNow:                 func() time.Time { return now },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return definition.NewWorkflowKey("system", "dlq", "run") },
		HandleGetExecutionInfo:    func() *persistencespb.WorkflowExecutionInfo { return &persistencespb.WorkflowExecutionInfo{} },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	root := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(t.Context(), root)
	req := componentRequest()
	message, err := NewMessage(ctx, req)
	require.NoError(t, err)
	require.NoError(t, root.SetRootComponent(message))
	original, err := message.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, now, original.Ref.EnqueuedAt)
	require.Equal(t, chasm.LifecycleStateRunning, message.LifecycleState(ctx))
	_, err = root.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	nodes := root.Snapshot(nil).Nodes
	require.Len(t, nodes, 3)
	dataNodes := 0
	for _, node := range nodes {
		if node.GetMetadata().GetDataAttributes() != nil {
			dataNodes++
		}
		if attrs := node.GetMetadata().GetComponentAttributes(); attrs != nil {
			require.Empty(t, attrs.GetPureTasks(), "pending messages must not schedule expiration")
		}
	}
	require.Equal(t, 1, dataNodes)
	restored, err := chasm.NewTreeFromDB(nodes, registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	readCtx := chasm.NewContext(t.Context(), restored)
	component, err := restored.Component(readCtx, chasm.ComponentRef{})
	require.NoError(t, err)
	actual, err := component.(*Message).Read(readCtx)
	require.NoError(t, err)
	require.Equal(t, original.Ref, actual.Ref)
	protorequire.ProtoEqual(t, req.Task, actual.Task)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, actual.Status)
	// Input and read results must not alias persisted state.
	req.Task.Blob.Data[0] = 'x'
	actual.Task.Blob.Data[0] = 'y'
	again, err := message.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("task payload"), again.Task.Blob.Data)
	require.IsType(t, &emptypb.Empty{}, message.Memo(ctx))
	require.Zero(t, proto.Size(message.Memo(ctx)))
	require.Equal(t, []chasm.SearchAttributeKeyValue{
		categorySearchAttribute.Value(int64(1)), sourceSearchAttribute.Value("source"), targetSearchAttribute.Value("target"),
		messageIDSearchAttribute.Value(original.Ref.MessageID), enqueuedAtSearchAttribute.Value(now), pendingSearchAttribute.Value(true),
	}, message.SearchAttributes(ctx))
}

func TestMessageResolution(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	enqueueTimeCutoff := start.Add(15 * time.Minute)
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
		t.Run(reason.String(), func(t *testing.T) {
			t.Parallel()
			now := start
			ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return now }}}
			message, err := NewMessage(ctx, componentRequest())
			require.NoError(t, err)
			_, err = message.Terminate(ctx, chasm.TerminateComponentRequest{})
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			require.Equal(t, chasm.LifecycleStateRunning, message.LifecycleState(ctx))
			require.Error(t, message.Resolve(ctx, reason, start.Add(-time.Nanosecond)))
			require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, message.GetStatus())
			now = start.Add(time.Hour)
			require.Error(t, message.Resolve(ctx, historydlqpb.Resolution(99), enqueueTimeCutoff))
			require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, message.GetStatus())
			require.NoError(t, message.Resolve(ctx, reason, enqueueTimeCutoff))
			require.Equal(t, chasm.LifecycleStateCompleted, message.LifecycleState(ctx))
			require.Equal(t, reason, message.GetResolution())
			require.Equal(t, now, message.GetResolvedAt().AsTime())
			require.NoError(t, message.Resolve(ctx, reason, enqueueTimeCutoff))
			require.Equal(t, reason, message.GetResolution())
			_, err = message.Terminate(ctx, chasm.TerminateComponentRequest{})
			require.NoError(t, err)
		})
	}
}

func TestMessagePayloadValidation(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC) }}}
	for _, size := range []int{1 << 20, 2 << 20, 3 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			t.Parallel()
			req := componentRequest()
			req.Task.Blob.Data = bytes.Repeat([]byte{42}, size)
			message, err := NewMessage(ctx, req)
			require.NoError(t, err)
			stored, err := message.Read(ctx)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
		})
	}
	for name, change := range map[string]func(*EnqueueRequest){
		"missing task":         func(r *EnqueueRequest) { r.Task = nil },
		"missing blob":         func(r *EnqueueRequest) { r.Task.Blob = nil },
		"empty blob":           func(r *EnqueueRequest) { r.Task.Blob.Data = nil },
		"unsupported encoding": func(r *EnqueueRequest) { r.Task.Blob.EncodingType = enumspb.ENCODING_TYPE_UNSPECIFIED },
		"wrong source shard":   func(r *EnqueueRequest) { r.Task.ShardId++ },
		"missing queue":        func(r *EnqueueRequest) { r.Queue = QueueKey{} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req := componentRequest()
			change(&req)
			message, err := NewMessage(ctx, req)
			require.Error(t, err)
			require.Nil(t, message)
		})
	}
}

func TestMessageJSONPayload(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC) }}}
	req := componentRequest()
	req.Task.Blob.EncodingType = enumspb.ENCODING_TYPE_JSON
	req.Task.Blob.Data = []byte(`{"taskId":123}`)
	message, err := NewMessage(ctx, req)
	require.NoError(t, err)
	stored, err := message.Read(ctx)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, req.Task, stored.Task)
}

func TestSerializedTaskPersistence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	key := definition.NewWorkflowKey("namespace", "workflow", "run")
	cases := map[string]tasks.Task{
		"workflow replication": &tasks.HistoryReplicationTask{WorkflowKey: key, TaskID: 123, VisibilityTimestamp: now, BranchToken: bytes.Repeat([]byte{42}, 1<<20)},
		"CHASM transfer": &tasks.ChasmTask{WorkflowKey: key, TaskID: 123, VisibilityTimestamp: now, Category: tasks.CategoryTransfer,
			Info: &persistencespb.ChasmTaskInfo{ArchetypeId: 42, Data: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: bytes.Repeat([]byte{42}, 1<<20)}}},
	}
	for name, task := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			serializer := serialization.NewSerializer()
			blob, err := serializer.SerializeTask(task)
			require.NoError(t, err)
			require.Greater(t, len(blob.Data), 1<<20)
			req := componentRequest()
			req.Queue.CategoryID = int32(task.GetCategory().ID())
			req.Task.Blob = blob
			req.SourceTaskIdentity.ArchetypeID = 0
			if typed, ok := task.(tasks.HasArchetypeID); ok {
				req.SourceTaskIdentity.ArchetypeID = typed.GetArchetypeID()
			}
			registry := chasm.NewRegistry(log.NewTestLogger())
			require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
			require.NoError(t, registry.Register(NewNilLibrary()))
			backend := &chasm.MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }, HandleNow: func() time.Time { return now }, HandleGetWorkflowKey: func() definition.WorkflowKey { return key }, HandleGetExecutionInfo: func() *persistencespb.WorkflowExecutionInfo { return &persistencespb.WorkflowExecutionInfo{} }}
			root := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
			ctx := chasm.NewMutableContext(t.Context(), root)
			message, err := NewMessage(ctx, req)
			require.NoError(t, err)
			require.NoError(t, root.SetRootComponent(message))
			_, err = root.CloseTransaction(t.Context(), true)
			require.NoError(t, err)
			restored, err := chasm.NewTreeFromDB(root.Snapshot(nil).Nodes, registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			readCtx := chasm.NewContext(t.Context(), restored)
			component, err := restored.Component(readCtx, chasm.ComponentRef{})
			require.NoError(t, err)
			stored, err := component.(*Message).Read(readCtx)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			decoded, err := serializer.DeserializeTask(task.GetCategory(), stored.Task.Blob)
			require.NoError(t, err)
			require.IsType(t, task, decoded)
		})
	}
}

func TestMissingTask(t *testing.T) {
	t.Parallel()
	message := &Message{MessageState: &historydlqpb.MessageState{Status: historydlqpb.MESSAGE_STATUS_PENDING}}
	_, err := message.Read(&chasm.MockContext{})
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}

func TestResolutionUsesCapturedCutoffAfterClockRollback(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	now := start.Add(-3*time.Hour - 10*time.Minute)
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return now }}}
	message, err := NewMessage(ctx, componentRequest())
	require.NoError(t, err)
	now = start.Add(-20 * time.Minute)
	enqueueTimeCutoff := start.Add(-3 * time.Hour)
	require.NoError(t, message.Resolve(ctx, historydlqpb.RESOLUTION_PURGED, enqueueTimeCutoff))
	require.Equal(t, chasm.LifecycleStateCompleted, message.LifecycleState(ctx))
}

func TestFixedCutoff(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	now := start.Add(-30 * time.Minute)
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return now }}}
	message, err := NewMessage(ctx, componentRequest())
	require.NoError(t, err)
	enqueueTimeCutoff := start.Add(-45 * time.Minute)
	for _, retryAt := range []time.Time{start, start.Add(4 * time.Hour)} {
		now = retryAt
		err = message.Resolve(ctx, historydlqpb.RESOLUTION_PURGED, enqueueTimeCutoff)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, message.GetStatus())
		require.Nil(t, message.GetResolvedAt())
	}
}
