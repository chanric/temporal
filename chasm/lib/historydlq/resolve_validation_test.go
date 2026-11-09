package historydlq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestResolveRecreatedEntryByLocator(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*MessageRef)
	}{
		{"original reference", func(*MessageRef) {}},
		{"locator only", func(ref *MessageRef) { ref.MessageID = 0; ref.EnqueuedAt = time.Time{} }},
		{"invalid metadata", func(ref *MessageRef) {
			ref.MessageID = -1
			ref.EnqueuedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, _, ts := newHandlerTestEnvironment(t)
			client := NewClient(log.NewNoopLogger(), testRPCClient{enqueue: h.Enqueue, read: h.ReadMessage, resolve: h.Resolve})
			req := testEnqueueRequest()
			original, err := client.Enqueue(ctx, req)
			require.NoError(t, err)
			require.NoError(t, client.Resolve(ctx, original, historydlqpb.RESOLUTION_PURGED))
			ts.Update(ts.Now().Add(time.Hour))
			recreated, err := client.Enqueue(ctx, req)
			require.NoError(t, err)
			req.SourceTaskIdentity.TaskID++
			other, err := client.Enqueue(ctx, req)
			require.NoError(t, err)
			_, err = client.ReadMessage(ctx, original)
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			tc.change(&original)
			require.NoError(t, client.Resolve(ctx, original, historydlqpb.RESOLUTION_MERGED))
			require.NoError(t, client.Resolve(ctx, original, historydlqpb.RESOLUTION_PURGED))
			_, err = client.ReadMessage(ctx, recreated)
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
			stored, err := client.ReadMessage(ctx, other)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
		})
	}
}

func TestResolveAPIIgnoresReferenceMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*historydlqpb.MessageRef)
	}{
		{"omitted", func(ref *historydlqpb.MessageRef) { ref.EnqueuedAt = nil; ref.MessageId = 0 }},
		{"invalid", func(ref *historydlqpb.MessageRef) {
			ref.EnqueuedAt = &timestamppb.Timestamp{Seconds: 1 << 62}
			ref.MessageId = -1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, _, _ := newHandlerTestEnvironment(t)
			accepted, err := h.Enqueue(ctx, testEnqueueRequestProto(t))
			require.NoError(t, err)
			locator := proto.Clone(accepted.Ref).(*historydlqpb.MessageRef)
			tc.change(locator)
			_, err = h.Resolve(ctx, &historydlqpb.ResolveRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: locator, Resolution: historydlqpb.RESOLUTION_PURGED})
			require.NoError(t, err)
			_, err = h.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: accepted.Ref})
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
		})
	}
}
