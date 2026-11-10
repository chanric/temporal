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
)

type resolveCountingEngine struct {
	chasm.Engine
	calls int
}

func (e *resolveCountingEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	update func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) ([]byte, error) {
	e.calls++
	return e.Engine.UpdateComponent(ctx, ref, update, opts...)
}

func TestResolveEntriesRetryAfterLostResponse(t *testing.T) {
	t.Parallel()
	handler, ctx, engine, _ := newHandlerTestEnvironment(t)
	counting := &resolveCountingEngine{Engine: engine}
	lost := false
	client := NewClient(log.NewNoopLogger(), testRPCClient{
		enqueue:  handler.Enqueue,
		describe: handler.DescribeSegment,
		resolveEntries: func(ctx context.Context, req *historydlqpb.ResolveEntriesRequest) (*historydlqpb.ResolveEntriesResponse, error) {
			response, err := handler.ResolveEntries(chasm.NewEngineContext(ctx, counting), req)
			if err == nil && !lost {
				lost = true
				return nil, serviceerror.NewUnavailable("response lost")
			}
			return response, err
		},
	})
	request := testEnqueueRequest()
	var refs []MessageRef
	for _, taskID := range []int64{7115, 7134} {
		request.SourceTaskIdentity.TaskID = taskID
		accepted, err := client.Enqueue(ctx, request)
		require.NoError(t, err)
		refs = append(refs, accepted)
	}
	resolve := resolutionRequest(refs[0], refs[0].EntryKey, refs[1].EntryKey, refs[0].EntryKey)
	require.ErrorAs(t, client.ResolveEntries(ctx, resolve), new(*serviceerror.Unavailable))
	require.Equal(t, 1, counting.calls)
	require.NoError(t, client.ResolveEntries(ctx, resolve))
	require.Equal(t, 2, counting.calls)
	stats, err := client.DescribeSegment(ctx, resolve.Segment)
	require.NoError(t, err)
	require.Equal(t, SegmentStats{}, stats)
	// A retry still covers a later admission of the same original task locator.
	accepted, err := client.Enqueue(ctx, request)
	require.NoError(t, err)
	require.Equal(t, refs[1].EntryKey, accepted.EntryKey)
	require.NoError(t, client.ResolveEntries(ctx, resolve))
	stats, err = client.DescribeSegment(ctx, resolve.Segment)
	require.NoError(t, err)
	require.Equal(t, SegmentStats{}, stats)
}

func TestResolveEntriesClientValidation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"empty key", "too many keys", "reason", "run", "business", "queue"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			ref, err := messageProtoToRef(testMessageRefProto(t))
			require.NoError(t, err)
			request := resolutionRequest(ref, ref.EntryKey)
			switch kind {
			case "empty key":
				request.EntryKeys = append(request.EntryKeys, "")
			case "too many keys":
				request.EntryKeys = make([]string, 1001)
			case "reason":
				request.Resolution = historydlqpb.RESOLUTION_UNSPECIFIED
			case "run":
				request.Segment.RunID = ""
			case "business":
				request.Segment.BusinessID = ""
			case "queue":
				request.Segment.Queue = QueueKey{}
			default:
				t.Fatalf("unknown test case: %s", kind)
			}
			client := NewClient(log.NewNoopLogger(), testRPCClient{})
			require.ErrorAs(t, client.ResolveEntries(t.Context(), request), new(*serviceerror.InvalidArgument))
		})
	}
}

func TestResolveEntriesHandlerValidation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"namespace", "target", "queue", "run", "business", "empty key", "reason", "nil"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			handler, ctx, _, _ := newHandlerTestEnvironment(t)
			accepted, err := handler.Enqueue(ctx, testEnqueueRequestProto(t))
			require.NoError(t, err)
			request := &historydlqpb.ResolveEntriesRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Segment: &historydlqpb.SegmentRef{
					Queue:      accepted.Ref.Queue,
					BusinessId: accepted.Ref.BusinessId,
					RunId:      accepted.Ref.RunId,
				},
				EntryKeys:  []string{accepted.Ref.EntryKey},
				Resolution: historydlqpb.RESOLUTION_PURGED,
			}
			switch kind {
			case "namespace":
				request.SystemNamespaceId = "other"
			case "target":
				request.Segment.Queue.TargetCluster = "other"
			case "queue":
				request.Segment.Queue.SourceCluster = "other"
			case "run":
				request.Segment.RunId = "other"
			case "business":
				request.Segment.BusinessId = "other"
			case "empty key":
				request.EntryKeys = append(request.EntryKeys, "")
			case "reason":
				request.Resolution = historydlqpb.RESOLUTION_UNSPECIFIED
			case "nil":
				request = nil
			default:
				t.Fatalf("unknown test case: %s", kind)
			}
			response, err := handler.ResolveEntries(ctx, request)
			require.Nil(t, response)
			switch kind {
			case "queue":
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			case "run", "business":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			}
		})
	}
}
