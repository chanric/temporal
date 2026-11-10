package historydlq

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
)

func TestMetadataClientRoutedRead(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	client := NewClient(log.NewNoopLogger(), testRPCClient{enqueue: h.Enqueue, read: h.ReadMessage, list: h.ListEntries, describe: h.DescribeSegment})
	accepted, err := client.Enqueue(ctx, testEnqueueRequest())
	require.NoError(t, err)
	ref := SegmentRef{Queue: accepted.Queue, BusinessID: accepted.BusinessID, RunID: accepted.RunID}
	page, err := client.ListEntries(ctx, ListEntriesRequest{Segment: ref, PageSize: 10, InclusiveMaxMessageID: math.MaxInt64})
	require.NoError(t, err)
	require.Equal(t, EntryPage{Refs: []MessageRef{accepted}}, page)
	stats, err := client.DescribeSegment(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, SegmentStats{PendingCount: 1, LastMessageID: accepted.MessageID}, stats)
	_, err = client.ReadMessage(ctx, accepted)
	require.NoError(t, err)
	ref.RunID = primitives.NewUUID().String()
	_, err = client.DescribeSegment(ctx, ref)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestMetadataClientRejectsInvalidResponse(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*historydlqpb.ListEntriesResponse){
		"empty ref":    func(r *historydlqpb.ListEntriesResponse) { r.Refs = []*historydlqpb.MessageRef{nil} },
		"wrong run":    func(r *historydlqpb.ListEntriesResponse) { r.Refs[0].RunId = "other" },
		"wrong queue":  func(r *historydlqpb.ListEntriesResponse) { r.Refs[0].Queue.SourceCluster = "other" },
		"out of bound": func(r *historydlqpb.ListEntriesResponse) { r.Refs[0].MessageId = 11 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			wire := testMessageRefProto(t)
			ref := SegmentRef{Queue: queueProtoToKey(wire.Queue), BusinessID: wire.BusinessId, RunID: wire.RunId}
			response := &historydlqpb.ListEntriesResponse{Refs: []*historydlqpb.MessageRef{wire}}
			change(response)
			c := NewClient(log.NewNoopLogger(), testRPCClient{list: func(context.Context, *historydlqpb.ListEntriesRequest) (*historydlqpb.ListEntriesResponse, error) {
				return response, nil
			}})
			_, err := c.ListEntries(t.Context(), ListEntriesRequest{Segment: ref, PageSize: 10, InclusiveMaxMessageID: 10})
			require.ErrorAs(t, err, new(*serviceerror.DataLoss))
		})
	}
	for name, response := range map[string]*historydlqpb.DescribeSegmentResponse{"nil": nil, "negative count": {PendingCount: -1}, "negative last": {LastMessageId: -1}, "empty with last": {LastMessageId: 1}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			wire := testMessageRefProto(t)
			c := NewClient(log.NewNoopLogger(), testRPCClient{describe: func(context.Context, *historydlqpb.DescribeSegmentRequest) (*historydlqpb.DescribeSegmentResponse, error) {
				return response, nil
			}})
			_, err := c.DescribeSegment(t.Context(), SegmentRef{Queue: queueProtoToKey(wire.Queue), BusinessID: wire.BusinessId, RunID: wire.RunId})
			require.ErrorAs(t, err, new(*serviceerror.DataLoss))
		})
	}
}

func TestMetadataHandlerValidatesRoutingAndQueue(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"namespace", "target", "queue", "run", "nil"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h, ctx, _, _ := newHandlerTestEnvironment(t)
			accepted, err := h.Enqueue(ctx, testEnqueueRequestProto(t))
			require.NoError(t, err)
			request := &historydlqpb.ListEntriesRequest{SystemNamespaceId: primitives.SystemNamespaceID, Segment: &historydlqpb.SegmentRef{Queue: accepted.Ref.Queue, BusinessId: accepted.Ref.BusinessId, RunId: accepted.Ref.RunId}, PageSize: 1, InclusiveMaxMessageId: math.MaxInt64}
			switch name {
			case "namespace":
				request.SystemNamespaceId = "other"
			case "target":
				request.Segment.Queue.TargetCluster = "other"
			case "queue":
				request.Segment.Queue.SourceCluster = "other"
			case "run":
				request.Segment.RunId = primitives.NewUUID().String()
			case "nil":
				request = nil
			default:
				t.Fatalf("unknown test case: %s", name)
			}
			_, err = h.ListEntries(ctx, request)
			switch name {
			case "namespace", "target", "nil":
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			case "queue":
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			case "run":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				t.Fatalf("unknown test case: %s", name)
			}
		})
	}
}

func TestSegmentClientsRejectMalformedCursor(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"metadata", "messages"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			handler, ctx, _, _ := newHandlerTestEnvironment(t)
			client := NewClient(log.NewNoopLogger(), testRPCClient{
				enqueue:     handler.Enqueue,
				list:        handler.ListEntries,
				readSegment: handler.ReadSegmentMessages,
			})
			accepted, err := client.Enqueue(ctx, testEnqueueRequest())
			require.NoError(t, err)
			request := ListEntriesRequest{
				Segment: SegmentRef{
					Queue:      accepted.Queue,
					BusinessID: accepted.BusinessID,
					RunID:      accepted.RunID,
				},
				InclusiveMaxMessageID: math.MaxInt64,
				PageSize:              1,
				NextPageToken:         []byte("malformed"),
			}
			switch operation {
			case "metadata":
				_, err = client.ListEntries(ctx, request)
			case "messages":
				_, err = client.ReadSegmentMessages(ctx, ReadSegmentMessagesRequest(request))
			default:
				t.Fatalf("unknown read operation: %s", operation)
			}
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}
