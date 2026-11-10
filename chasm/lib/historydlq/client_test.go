package historydlq

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	routingspb "go.temporal.io/server/api/routing/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testRPCClient struct {
	readSegment func(context.Context, *historydlqpb.ReadSegmentMessagesRequest) (*historydlqpb.ReadSegmentMessagesResponse, error)
	historydlqpb.HistoryDLQServiceClient
	enqueue  func(context.Context, *historydlqpb.EnqueueRequest) (*historydlqpb.EnqueueResponse, error)
	read     func(context.Context, *historydlqpb.ReadMessageRequest) (*historydlqpb.ReadMessageResponse, error)
	resolve  func(context.Context, *historydlqpb.ResolveRequest) (*historydlqpb.ResolveResponse, error)
	list     func(context.Context, *historydlqpb.ListEntriesRequest) (*historydlqpb.ListEntriesResponse, error)
	describe func(context.Context, *historydlqpb.DescribeSegmentRequest) (*historydlqpb.DescribeSegmentResponse, error)
}

func (c testRPCClient) Enqueue(
	ctx context.Context,
	req *historydlqpb.EnqueueRequest,
	_ ...grpc.CallOption,
) (*historydlqpb.EnqueueResponse, error) {
	return c.enqueue(ctx, req)
}

func (c testRPCClient) ReadMessage(
	ctx context.Context,
	req *historydlqpb.ReadMessageRequest,
	_ ...grpc.CallOption,
) (*historydlqpb.ReadMessageResponse, error) {
	return c.read(ctx, req)
}

func (c testRPCClient) Resolve(
	ctx context.Context,
	req *historydlqpb.ResolveRequest,
	_ ...grpc.CallOption,
) (*historydlqpb.ResolveResponse, error) {
	return c.resolve(ctx, req)
}

func testMessageRefProto(t *testing.T) *historydlqpb.MessageRef {
	t.Helper()
	req := testEnqueueRequest()
	id, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	entryKey, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	return &historydlqpb.MessageRef{
		EntryKey:   entryKey,
		BusinessId: id,
		RunId:      primitives.NewUUID().String(),
		Queue: &commonspb.HistoryDLQKey{
			TaskCategory:  1,
			SourceCluster: "source",
			TargetCluster: "target",
		},
		EnqueuedAt: timestamppb.New(time.Date(2026, 11, 5, 12, 0, 0, 123, time.UTC)),
	}
}

func TestClientRequestConversion(t *testing.T) {
	t.Parallel()
	for _, id := range []int64{0, 1 << 61, 2 << 61, 3 << 61} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			t.Parallel()
			ref := testMessageRefProto(t)
			ref.MessageId = id
			req := testEnqueueRequest()
			original := &historydlqpb.TaskIdentity{
				SourceShardId: 7,
				TaskId:        123,
				NamespaceId:   "namespace",
				BusinessId:    "workflow",
				RunId:         "run",
				ArchetypeId:   1,
			}

			client := NewClient(log.NewNoopLogger(), testRPCClient{
				enqueue: func(_ context.Context, got *historydlqpb.EnqueueRequest) (*historydlqpb.EnqueueResponse, error) {
					protorequire.ProtoEqual(t, &historydlqpb.EnqueueRequest{
						SystemNamespaceId:  primitives.SystemNamespaceID,
						BusinessId:         ref.BusinessId,
						Queue:              ref.Queue,
						SourceTaskIdentity: original,
						Task:               req.Task,
					}, got)
					return &historydlqpb.EnqueueResponse{Ref: ref}, nil
				},
				read: func(_ context.Context, got *historydlqpb.ReadMessageRequest) (*historydlqpb.ReadMessageResponse, error) {
					protorequire.ProtoEqual(t, &historydlqpb.ReadMessageRequest{
						SystemNamespaceId: primitives.SystemNamespaceID,
						Ref:               ref,
					}, got)
					return &historydlqpb.ReadMessageResponse{
						Message: &historydlqpb.StoredMessage{
							Ref:                ref,
							SourceTaskIdentity: original,
							Task:               req.Task,
						},
					}, nil
				},
				resolve: func(_ context.Context, got *historydlqpb.ResolveRequest) (*historydlqpb.ResolveResponse, error) {
					protorequire.ProtoEqual(t, &historydlqpb.ResolveRequest{
						SystemNamespaceId: primitives.SystemNamespaceID,
						Ref:               ref,
						Resolution:        historydlqpb.RESOLUTION_PURGED,
					}, got)
					return &historydlqpb.ResolveResponse{}, nil
				},
			})
			appRef, err := client.Enqueue(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, MessageRef{
				BusinessID: ref.BusinessId,
				RunID:      ref.RunId,
				MessageID:  id,
				EntryKey:   ref.EntryKey,
				Queue:      req.Queue,
				EnqueuedAt: ref.EnqueuedAt.AsTime(),
			}, appRef)
			stored, err := client.ReadMessage(t.Context(), appRef)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			stored.Task.Blob.Data[0] = 'x'
			require.Equal(t, []byte("task payload"), req.Task.Blob.Data)
			stored.Task = nil
			require.Equal(t, StoredMessage{
				Ref:                appRef,
				SourceTaskIdentity: req.SourceTaskIdentity,
			}, stored)
			require.NoError(t, client.Resolve(t.Context(), appRef, historydlqpb.RESOLUTION_PURGED))
		})
	}
}

func TestClientResponseConversion(t *testing.T) {
	t.Parallel()
	for name, response := range map[string]*historydlqpb.MessageRef{
		"missing":       nil,
		"empty":         {},
		"bad timestamp": {EnqueuedAt: &timestamppb.Timestamp{Seconds: 1 << 62}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := NewClient(log.NewNoopLogger(), testRPCClient{enqueue: func(
				context.Context,
				*historydlqpb.EnqueueRequest,
			) (*historydlqpb.EnqueueResponse, error) {
				return &historydlqpb.EnqueueResponse{Ref: response}, nil
			}})
			_, err := client.Enqueue(t.Context(), testEnqueueRequest())
			require.ErrorAs(t, err, new(*serviceerror.DataLoss))
		})
	}
	ref := testMessageRefProto(t)
	appRef := MessageRef{
		BusinessID: ref.BusinessId,
		RunID:      ref.RunId,
		Queue:      testEnqueueRequest().Queue,
		EnqueuedAt: ref.EnqueuedAt.AsTime(),
		EntryKey:   ref.EntryKey,
	}
	errUnavailable := serviceerror.NewUnavailable("injected")
	client := NewClient(log.NewNoopLogger(), testRPCClient{
		enqueue: func(context.Context, *historydlqpb.EnqueueRequest) (*historydlqpb.EnqueueResponse, error) {
			return nil, errUnavailable
		},
		read: func(context.Context, *historydlqpb.ReadMessageRequest) (*historydlqpb.ReadMessageResponse, error) {
			return nil, errUnavailable
		},
		resolve: func(context.Context, *historydlqpb.ResolveRequest) (*historydlqpb.ResolveResponse, error) {
			return nil, errUnavailable
		},
	})
	_, err := client.Enqueue(t.Context(), testEnqueueRequest())
	require.ErrorIs(t, err, errUnavailable)
	_, err = client.ReadMessage(t.Context(), appRef)
	require.ErrorIs(t, err, errUnavailable)
	require.ErrorIs(t, client.Resolve(t.Context(), appRef, historydlqpb.RESOLUTION_PURGED), errUnavailable)
	_, err = client.ReadMessage(t.Context(), MessageRef{})
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_UNSPECIFIED, historydlqpb.Resolution(99)} {
		require.ErrorAs(t, client.Resolve(t.Context(), appRef, reason), new(*serviceerror.InvalidArgument))
	}
	for name, response := range map[string]*historydlqpb.ReadMessageResponse{
		"nil":             nil,
		"missing message": {},
		"empty message":   {Message: &historydlqpb.StoredMessage{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := NewClient(log.NewNoopLogger(), testRPCClient{read: func(
				context.Context,
				*historydlqpb.ReadMessageRequest,
			) (*historydlqpb.ReadMessageResponse, error) {
				return response, nil
			}})
			_, err := client.ReadMessage(t.Context(), appRef)
			require.ErrorAs(t, err, new(*serviceerror.Internal))
		})
	}
}

func TestRPCRoutingDescriptors(t *testing.T) {
	t.Parallel()
	ref := testMessageRefProto(t)
	requests := []proto.Message{
		&historydlqpb.EnqueueRequest{SystemNamespaceId: primitives.SystemNamespaceID, BusinessId: ref.BusinessId},
		&historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               ref,
		},
		&historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               ref,
		},
	}
	requests = append(requests,
		&historydlqpb.ListEntriesRequest{SystemNamespaceId: primitives.SystemNamespaceID, Segment: &historydlqpb.SegmentRef{BusinessId: ref.BusinessId, RunId: ref.RunId, Queue: ref.Queue}},
		&historydlqpb.DescribeSegmentRequest{SystemNamespaceId: primitives.SystemNamespaceID, Segment: &historydlqpb.SegmentRef{BusinessId: ref.BusinessId, RunId: ref.RunId, Queue: ref.Queue}},
	)
	requests = append(requests, &historydlqpb.ReadSegmentMessagesRequest{SystemNamespaceId: primitives.SystemNamespaceID, Segment: &historydlqpb.SegmentRef{BusinessId: ref.BusinessId, RunId: ref.RunId, Queue: ref.Queue}})
	methods := historydlqpb.File_temporal_server_chasm_lib_historydlq_proto_v1_service_proto.Services().Get(0).Methods()
	get := func(msg protoreflect.Message, path string) string {
		parts := strings.Split(path, ".")
		for i, part := range parts {
			field := msg.Descriptor().Fields().ByName(protoreflect.Name(part))
			require.NotNil(t, field)
			value := msg.Get(field)
			if i == len(parts)-1 {
				return value.String()
			}
			msg = value.Message()
		}
		panic("empty path")
	}
	for i, req := range requests {
		options := proto.GetExtension(methods.Get(i).Options(), routingspb.E_Routing).(*routingspb.RoutingOptions)
		key := chasm.ExecutionKey{
			NamespaceID: get(req.ProtoReflect(), options.GetNamespaceId()),
			BusinessID:  get(req.ProtoReflect(), options.GetBusinessId()[0]),
		}
		require.Equal(t, chasm.ExecutionKey{NamespaceID: primitives.SystemNamespaceID, BusinessID: ref.BusinessId}, key)
		for _, shards := range []int32{1, 4, 32, 512, 4096} {
			require.Equal(
				t,
				common.WorkflowIDToHistoryShard(primitives.SystemNamespaceID, ref.BusinessId, shards),
				common.WorkflowIDToHistoryShard(key.NamespaceID, key.BusinessID, shards),
			)
		}
	}
}

func TestClientStoredEntryValidation(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*historydlqpb.StoredMessage){
		"missing identity":          func(m *historydlqpb.StoredMessage) { m.SourceTaskIdentity = nil },
		"different entry":           func(m *historydlqpb.StoredMessage) { m.SourceTaskIdentity.TaskId++ },
		"different producer bucket": func(m *historydlqpb.StoredMessage) { m.SourceTaskIdentity.NamespaceId += "-other" },
		"wrong shard":               func(m *historydlqpb.StoredMessage) { m.Task.ShardId++ },
		"empty payload":             func(m *historydlqpb.StoredMessage) { m.Task.Blob.Data = nil },
		"unsupported encoding":      func(m *historydlqpb.StoredMessage) { m.Task.Blob.EncodingType = 0 },
		"different ref":             func(m *historydlqpb.StoredMessage) { m.Ref.EntryKey = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			wire := testMessageRefProto(t)
			ref, err := messageProtoToRef(wire)
			require.NoError(t, err)
			req := testEnqueueRequest()
			message := &historydlqpb.StoredMessage{
				Ref:                proto.Clone(wire).(*historydlqpb.MessageRef),
				SourceTaskIdentity: taskIdentityToProto(req.SourceTaskIdentity),
				Task:               req.Task,
			}
			change(message)
			core, recorded := observer.New(zap.ErrorLevel)
			client := NewClient(log.NewZapLogger(zap.New(core)), testRPCClient{read: func(
				context.Context,
				*historydlqpb.ReadMessageRequest,
			) (*historydlqpb.ReadMessageResponse, error) {
				return &historydlqpb.ReadMessageResponse{Message: message}, nil
			}})
			stored, err := client.ReadMessage(t.Context(), ref)
			require.ErrorAs(t, err, new(*serviceerror.Internal))
			require.Equal(t, StoredMessage{}, stored)
			entries := recorded.All()
			require.Len(t, entries, 1)
			require.Equal(t, true, entries[0].ContextMap()["failed-assertion"])
		})
	}
}

func TestClientReadRequiresTask(t *testing.T) {
	t.Parallel()
	wire := testMessageRefProto(t)
	ref, err := messageProtoToRef(wire)
	require.NoError(t, err)
	req := testEnqueueRequest()
	core, recorded := observer.New(zap.ErrorLevel)
	client := NewClient(log.NewZapLogger(zap.New(core)), testRPCClient{read: func(
		context.Context,
		*historydlqpb.ReadMessageRequest,
	) (*historydlqpb.ReadMessageResponse, error) {
		return &historydlqpb.ReadMessageResponse{
			Message: &historydlqpb.StoredMessage{
				Ref:                wire,
				SourceTaskIdentity: taskIdentityToProto(req.SourceTaskIdentity),
			},
		}, nil
	}})
	stored, err := client.ReadMessage(t.Context(), ref)
	require.ErrorAs(t, err, new(*serviceerror.Internal))
	require.Len(t, recorded.All(), 1)
	require.Equal(t, StoredMessage{}, stored)
}

func TestClientEnqueueRejectsDifferentEntry(t *testing.T) {
	t.Parallel()
	wire := testMessageRefProto(t)
	wire.EntryKey = "other"
	client := NewClient(log.NewNoopLogger(), testRPCClient{enqueue: func(
		context.Context,
		*historydlqpb.EnqueueRequest,
	) (*historydlqpb.EnqueueResponse, error) {
		return &historydlqpb.EnqueueResponse{Ref: wire}, nil
	}})
	_, err := client.Enqueue(t.Context(), testEnqueueRequest())
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}

func TestClientProducerBuckets(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	client := NewClient(log.NewNoopLogger(), testRPCClient{enqueue: h.Enqueue, read: h.ReadMessage})
	segments := make(map[string]struct{})
	for _, fixture := range []struct {
		namespaceID string
		archetypeID uint32
		businessID  string
		bucketID    uint32
	}{
		{"namespace-9", 9, "business-9", 0},
		{"namespace-0", 0, "business-0", 1},
		{"namespace-2", 2, "business-2", 2},
		{"namespace-1", 1, "business-1", 3},
	} {
		req := testEnqueueRequest()
		req.SourceTaskIdentity.NamespaceID = fixture.namespaceID
		req.SourceTaskIdentity.ArchetypeID = fixture.archetypeID
		req.SourceTaskIdentity.BusinessID = fixture.businessID
		require.Equal(t, fixture.bucketID, SegmentBucketID(req.SourceTaskIdentity))
		first, err := client.Enqueue(ctx, req)
		require.NoError(t, err)
		require.NotContains(t, segments, first.BusinessID)
		segments[first.BusinessID] = struct{}{}
		req.SourceTaskIdentity.TaskID += 1 << 40
		req.SourceTaskIdentity.RunID = "next-run"
		second, err := client.Enqueue(ctx, req)
		require.NoError(t, err)
		require.Equal(t, first.BusinessID, second.BusinessID)
		require.Equal(t, first.RunID, second.RunID)
		require.NotEqual(t, first.EntryKey, second.EntryKey)
		stored, err := client.ReadMessage(ctx, second)
		require.NoError(t, err)
		require.Equal(t, req.SourceTaskIdentity, stored.SourceTaskIdentity)
		protorequire.ProtoEqual(t, req.Task, stored.Task)
	}
	require.Len(t, segments, 4)
}

func (c testRPCClient) ListEntries(ctx context.Context, req *historydlqpb.ListEntriesRequest, _ ...grpc.CallOption) (*historydlqpb.ListEntriesResponse, error) {
	return c.list(ctx, req)
}
func (c testRPCClient) DescribeSegment(ctx context.Context, req *historydlqpb.DescribeSegmentRequest, _ ...grpc.CallOption) (*historydlqpb.DescribeSegmentResponse, error) {
	return c.describe(ctx, req)
}

func (c testRPCClient) ReadSegmentMessages(ctx context.Context, req *historydlqpb.ReadSegmentMessagesRequest, _ ...grpc.CallOption) (*historydlqpb.ReadSegmentMessagesResponse, error) {
	return c.readSegment(ctx, req)
}
