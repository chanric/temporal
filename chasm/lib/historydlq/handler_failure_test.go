package historydlq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type controlledEngine struct {
	chasm.Engine
	updateError       error
	beforeCommitError error
	lostResponse      bool
	mismatch          bool
}

func (e *controlledEngine) UpdateWithStartExecution(
	ctx context.Context,
	ref chasm.ComponentRef,
	start func(chasm.MutableContext) (chasm.RootComponent, error),
	update func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (chasm.EngineUpdateWithStartExecutionResult, error) {
	if e.updateError != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, e.updateError
	}
	if e.beforeCommitError != nil {
		originalUpdate := update
		update = func(ctx chasm.MutableContext, component chasm.Component) error {
			if err := originalUpdate(ctx, component); err != nil {
				return err
			}
			return e.beforeCommitError
		}
	}
	result, err := e.Engine.UpdateWithStartExecution(ctx, ref, start, update, opts...)
	if err == nil && e.lostResponse {
		return chasm.EngineUpdateWithStartExecutionResult{}, serviceerror.NewUnavailable("response lost")
	}
	if e.mismatch {
		result.ExecutionKey.RunID = "other-run"
	}
	return result, err
}

func (e *controlledEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	update func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) ([]byte, error) {
	if e.updateError != nil {
		return nil, e.updateError
	}
	result, err := e.Engine.UpdateComponent(ctx, ref, update, opts...)
	if err == nil && e.lostResponse {
		return nil, serviceerror.NewUnavailable("response lost")
	}
	return result, err
}

func TestResolveRetryAfterLostResponse(t *testing.T) {
	t.Parallel()
	h, ctx, engine, _ := newHandlerTestEnvironment(t)
	accepted, err := h.Enqueue(ctx, testEnqueueRequestProto(t))
	require.NoError(t, err)
	req := &historydlqpb.ResolveRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               accepted.Ref,
		Resolution:        historydlqpb.RESOLUTION_PURGED,
	}
	_, err = h.Resolve(chasm.NewEngineContext(t.Context(), &controlledEngine{Engine: engine, lostResponse: true}), req)
	require.ErrorAs(t, err, new(*serviceerror.Unavailable))
	_, err = h.Resolve(ctx, req)
	require.NoError(t, err)
	_, err = h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               accepted.Ref,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestEnqueueEngineFailure(t *testing.T) {
	t.Parallel()
	h, _, engine, _ := newHandlerTestEnvironment(t)
	fault := serviceerror.NewUnavailable("persistence failed")
	response, err := h.Enqueue(chasm.NewEngineContext(t.Context(), &controlledEngine{
		Engine:      engine,
		updateError: fault,
	}), testEnqueueRequestProto(t))
	require.ErrorIs(t, err, fault)
	require.Nil(t, response)
}

func TestEnqueueRetryAfterLostResponse(t *testing.T) {
	t.Parallel()
	h, ctx, engine, _ := newHandlerTestEnvironment(t)
	req := testEnqueueRequestProto(t)
	response, err := h.Enqueue(chasm.NewEngineContext(t.Context(), &controlledEngine{
		Engine:       engine,
		lostResponse: true,
	}), req)
	require.Error(t, err)
	require.Nil(t, response)
	accepted, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	retry, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, accepted, retry)
}

func TestEnqueueRejectsUnacceptedOutput(t *testing.T) {
	t.Parallel()
	h, _, engine, _ := newHandlerTestEnvironment(t)
	core, recorded := observer.New(zap.ErrorLevel)
	h.logger = log.NewZapLogger(zap.New(core))
	response, err := h.Enqueue(chasm.NewEngineContext(t.Context(), &controlledEngine{
		Engine:   engine,
		mismatch: true,
	}), testEnqueueRequestProto(t))
	require.ErrorAs(t, err, new(*serviceerror.Internal))
	require.Nil(t, response)
	entries := recorded.All()
	require.Len(t, entries, 1)
	require.Equal(t, true, entries[0].ContextMap()["failed-assertion"])
}

func TestResolvePersistenceFailure(t *testing.T) {
	t.Parallel()
	h, ctx, engine, _ := newHandlerTestEnvironment(t)
	accepted, err := h.Enqueue(ctx, testEnqueueRequestProto(t))
	require.NoError(t, err)
	fault := serviceerror.NewUnavailable("update failed")
	response, err := h.Resolve(chasm.NewEngineContext(t.Context(), &controlledEngine{
		Engine:      engine,
		updateError: fault,
	}), &historydlqpb.ResolveRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               accepted.Ref,
		Resolution:        historydlqpb.RESOLUTION_PURGED,
	})
	require.ErrorIs(t, err, fault)
	require.Nil(t, response)
	stored, err := h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               accepted.Ref,
	})
	require.NoError(t, err)
	require.NotNil(t, stored.Message.Task)
}
