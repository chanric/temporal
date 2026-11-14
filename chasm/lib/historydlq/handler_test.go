package historydlq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The test engine requires callers to assign new execution Run IDs.
type handlerTestEngine struct {
	*chasmtest.Engine
	runs map[string]string
}

func (e *handlerTestEngine) UpdateWithStartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	start func(chasm.MutableContext) (chasm.RootComponent, error),
	update func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	key := ref.NamespaceID + "/" + ref.BusinessID
	run, ok := e.runs[key]
	if !ok {
		run = primitives.NewUUID().String()
	}
	ref.RunID = run
	result, err := e.Engine.UpdateWithStartExecution(ctx, ref, start, update, opts...)
	if err == nil {
		e.runs[key] = result.ExecutionKey.RunID
	}
	return result, err
}

func newHandlerTestEnvironment(t *testing.T) (*serviceHandler, context.Context, *handlerTestEngine, *clock.EventTimeSource) {
	t.Helper()
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 5, 12, 0, 0, 123, time.UTC))
	engine := &handlerTestEngine{
		Engine: chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts)),
		runs:   make(map[string]string),
	}
	metadata := cluster.NewMockMetadata(gomock.NewController(t))
	metadata.EXPECT().GetCurrentClusterName().Return("target").AnyTimes()
	return newServiceHandler(logger, metadata, metrics.NoopMetricsHandler), chasm.NewEngineContext(t.Context(), engine), engine, ts
}

func testEnqueueRequestProto(t *testing.T) *historydlqpb.EnqueueRequest {
	t.Helper()
	req := testEnqueueRequest()
	id, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	return &historydlqpb.EnqueueRequest{
		SystemNamespaceId:  primitives.SystemNamespaceID,
		BusinessId:         id,
		Queue:              queueKeyToProto(req.Queue),
		SourceTaskIdentity: taskIdentityToProto(req.SourceTaskIdentity),
		Task:               req.Task,
	}
}

func TestEnqueueValidation(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*historydlqpb.EnqueueRequest){
		"system":               func(r *historydlqpb.EnqueueRequest) { r.SystemNamespaceId = "other" },
		"business":             func(r *historydlqpb.EnqueueRequest) { r.BusinessId = "other" },
		"source shard routing": func(r *historydlqpb.EnqueueRequest) { r.SourceTaskIdentity.SourceShardId++; r.Task.ShardId++ },
		"queue":                func(r *historydlqpb.EnqueueRequest) { r.Queue = nil },
		"category":             func(r *historydlqpb.EnqueueRequest) { r.Queue.TaskCategory = 0 },
		"source":               func(r *historydlqpb.EnqueueRequest) { r.Queue.SourceCluster = "" },
		"target":               func(r *historydlqpb.EnqueueRequest) { r.Queue.TargetCluster = "other" },
		"original":             func(r *historydlqpb.EnqueueRequest) { r.SourceTaskIdentity = nil },
		"task ID":              func(r *historydlqpb.EnqueueRequest) { r.SourceTaskIdentity.TaskId = 0 },
		"shard":                func(r *historydlqpb.EnqueueRequest) { r.SourceTaskIdentity.SourceShardId = 0 },
		"different shard":      func(r *historydlqpb.EnqueueRequest) { r.Task.ShardId++ },
		"task":                 func(r *historydlqpb.EnqueueRequest) { r.Task = nil },
		"blob":                 func(r *historydlqpb.EnqueueRequest) { r.Task.Blob = nil },
		"empty blob":           func(r *historydlqpb.EnqueueRequest) { r.Task.Blob.Data = nil },
		"encoding":             func(r *historydlqpb.EnqueueRequest) { r.Task.Blob.EncodingType = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h, ctx, _, _ := newHandlerTestEnvironment(t)
			req := testEnqueueRequestProto(t)
			change(req)
			_, err := h.Enqueue(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
	h, _, _, _ := newHandlerTestEnvironment(t)
	_, err := h.Enqueue(t.Context(), nil)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
}

func TestExactRequestValidation(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*historydlqpb.MessageRef){
		"entry key":   func(r *historydlqpb.MessageRef) { r.EntryKey = "" },
		"business":    func(r *historydlqpb.MessageRef) { r.BusinessId = "" },
		"run":         func(r *historydlqpb.MessageRef) { r.RunId = "" },
		"queue":       func(r *historydlqpb.MessageRef) { r.Queue = nil },
		"time":        func(r *historydlqpb.MessageRef) { r.EnqueuedAt = nil },
		"bad time":    func(r *historydlqpb.MessageRef) { r.EnqueuedAt = &timestamppb.Timestamp{Seconds: 1 << 62} },
		"negative ID": func(r *historydlqpb.MessageRef) { r.MessageId = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h, _, _, _ := newHandlerTestEnvironment(t)
			ref := testMessageRefProto(t)
			change(ref)
			_, err := h.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               ref,
			})
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			if name == "time" || name == "bad time" || name == "negative ID" {
				return
			}
			_, err = h.Resolve(t.Context(), &historydlqpb.ResolveRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               ref,
				Resolution:        historydlqpb.RESOLUTION_PURGED,
			})
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
	h, _, _, _ := newHandlerTestEnvironment(t)
	_, err := h.ReadMessage(t.Context(), nil)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	_, err = h.Resolve(t.Context(), nil)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
}

func TestEnqueueRunningDuplicate(t *testing.T) {
	t.Parallel()
	h, ctx, _, ts := newHandlerTestEnvironment(t)
	req := testEnqueueRequestProto(t)
	first, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	ts.Update(ts.Now().Add(time.Hour))
	again, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, first, again)
}

func TestEnqueueExistingAfterClockRollback(t *testing.T) {
	t.Parallel()
	h, ctx, _, ts := newHandlerTestEnvironment(t)
	req := testEnqueueRequestProto(t)
	first, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	ts.Update(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	again, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, first, again)
}

func TestEnqueueExistingPayloadFirstWins(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	req := testEnqueueRequestProto(t)
	first, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	wantTask := proto.Clone(req.Task)
	wantIdentity := proto.Clone(req.SourceTaskIdentity)
	req.Task.Blob.Data = []byte("different payload")
	again, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, first, again)
	stored, err := h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               first.Ref,
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, wantTask, stored.Message.Task)
	protorequire.ProtoEqual(t, wantIdentity, stored.Message.SourceTaskIdentity)
}

func TestEnqueueAfterResolutionCreatesEntry(t *testing.T) {
	t.Parallel()
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
		t.Run(reason.String(), func(t *testing.T) {
			t.Parallel()
			h, ctx, _, ts := newHandlerTestEnvironment(t)
			req := testEnqueueRequestProto(t)
			first, err := h.Enqueue(ctx, req)
			require.NoError(t, err)
			_, err = h.Resolve(ctx, &historydlqpb.ResolveRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               first.Ref,
				Resolution:        reason,
			})
			require.NoError(t, err)
			_, err = h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               first.Ref,
			})
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
			ts.Update(ts.Now().Add(time.Hour))
			again, err := h.Enqueue(ctx, req)
			require.NoError(t, err)
			require.Equal(t, first.Ref.EntryKey, again.Ref.EntryKey)
			require.NotEqual(t, first.Ref.EnqueuedAt, again.Ref.EnqueuedAt)
			stored, err := h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               again.Ref,
			})
			require.NoError(t, err)
			require.NotNil(t, stored.Message.Task)
			protorequire.ProtoEqual(t, req.Task, stored.Message.Task)
		})
	}
}

func TestReadMessageExactReference(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	response, err := h.Enqueue(ctx, testEnqueueRequestProto(t))
	require.NoError(t, err)
	for name, change := range map[string]func(*historydlqpb.MessageRef){
		"queue": func(r *historydlqpb.MessageRef) { r.Queue.SourceCluster = "other" },
		"ID":    func(r *historydlqpb.MessageRef) { r.MessageId++ },
		"time":  func(r *historydlqpb.MessageRef) { r.EnqueuedAt.Nanos++ },
	} {
		t.Run(name, func(t *testing.T) {
			ref := proto.Clone(response.Ref).(*historydlqpb.MessageRef)
			change(ref)
			_, err := h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               ref,
			})
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			if name == "queue" {
				_, err = h.Resolve(ctx, &historydlqpb.ResolveRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Ref:               ref,
					Resolution:        historydlqpb.RESOLUTION_PURGED,
				})
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			}
		})
	}
	ref := proto.Clone(response.Ref).(*historydlqpb.MessageRef)
	ref.RunId = primitives.NewUUID().String()
	_, err = h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               ref,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestResolveFreshEntry(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	response, err := h.Enqueue(ctx, testEnqueueRequestProto(t))
	require.NoError(t, err)
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
		_, err = h.Resolve(ctx, &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               response.Ref,
			Resolution:        reason,
		})
		require.NoError(t, err)
	}
	_, err = h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               response.Ref,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestEnqueueAppendsToExistingSegment(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	req := testEnqueueRequestProto(t)
	first, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	req.SourceTaskIdentity.TaskId += 1 << 40
	req.SourceTaskIdentity.RunId = "next-run"
	second, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Equal(t, first.Ref.BusinessId, second.Ref.BusinessId)
	require.Equal(t, first.Ref.RunId, second.Ref.RunId)
	require.NotEqual(t, first.Ref.EntryKey, second.Ref.EntryKey)
	for _, ref := range []*historydlqpb.MessageRef{first.Ref, second.Ref} {
		stored, err := h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               ref,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, req.Task, stored.Message.Task)
	}
}

func TestNumericIDCollisionExactOperations(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	var refs []*historydlqpb.MessageRef
	for _, id := range []int64{909, 976} {
		req := testEnqueueRequestProto(t)
		req.SourceTaskIdentity.TaskId = id
		req.BusinessId, _ = SegmentBusinessID(queueProtoToKey(req.Queue), taskIdentityProtoToIdentity(req.SourceTaskIdentity))
		req.Task.Blob.Data = []byte(fmt.Sprint(id))
		r, err := h.Enqueue(ctx, req)
		require.NoError(t, err)
		refs = append(refs, r.Ref)
	}
	require.Equal(t, refs[0].MessageId, refs[1].MessageId)
	require.Equal(t, refs[0].BusinessId, refs[1].BusinessId)
	require.Equal(t, refs[0].RunId, refs[1].RunId)
	require.NotEqual(t, refs[0].EntryKey, refs[1].EntryKey)
	_, err := h.Resolve(ctx, &historydlqpb.ResolveRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               refs[0],
		Resolution:        historydlqpb.RESOLUTION_PURGED,
	})
	require.NoError(t, err)
	for i, ref := range refs {
		r, err := h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               ref,
		})
		if i == 0 {
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
			continue
		}
		require.NoError(t, err)
		require.Equal(t, []byte("976"), r.Message.Task.Blob.Data)
		require.NotNil(t, r.Message.Task)
	}
}

func TestExactRoutingValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, namespace, target string }{
		{"missing namespace", "", "target"},
		{"user namespace", "user", "target"},
		{"namespace name", "temporal-system", "target"},
		{"remote target", primitives.SystemNamespaceID, "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _, _, _ := newHandlerTestEnvironment(t)
			ref := testMessageRefProto(t)
			ref.Queue.TargetCluster = tc.target
			_, err := h.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{SystemNamespaceId: tc.namespace, Ref: ref})
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			_, err = h.Resolve(t.Context(), &historydlqpb.ResolveRequest{
				SystemNamespaceId: tc.namespace,
				Ref:               ref,
				Resolution:        historydlqpb.RESOLUTION_PURGED,
			})
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}
