package historydlq

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/fx/fxtest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type routingServer struct {
	historydlqpb.UnimplementedHistoryDLQServiceServer
	requests chan proto.Message
	fault    func() error
}

func (s *routingServer) Enqueue(
	_ context.Context,
	req *historydlqpb.EnqueueRequest,
) (*historydlqpb.EnqueueResponse, error) {
	s.requests <- req
	return &historydlqpb.EnqueueResponse{}, s.fault()
}

func (s *routingServer) ReadMessage(
	_ context.Context,
	req *historydlqpb.ReadMessageRequest,
) (*historydlqpb.ReadMessageResponse, error) {
	s.requests <- req
	return &historydlqpb.ReadMessageResponse{}, s.fault()
}

func (s *routingServer) Resolve(
	_ context.Context,
	req *historydlqpb.ResolveRequest,
) (*historydlqpb.ResolveResponse, error) {
	s.requests <- req
	return &historydlqpb.ResolveResponse{}, s.fault()
}

func newLayeredClientTestEnvironment(
	t *testing.T,
	caching bool,
	faults ...func() error,
) (historydlqpb.HistoryDLQServiceClient, []*routingServer, chan string, *metricstest.CaptureHandler) {
	t.Helper()
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockServiceResolver(ctrl)
	monitor := membership.NewMockMonitor(ctrl)
	monitor.EXPECT().GetResolver(primitives.HistoryService).Return(resolver, nil)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().Members().Return(nil).AnyTimes()
	shards := make(chan string, 20)
	resolver.EXPECT().Lookup(gomock.Any()).DoAndReturn(func(key string) (membership.HostInfo, error) {
		shards <- key
		return membership.NewHostInfoFromAddress("host0"), nil
	}).AnyTimes()
	factory := common.NewMockRPCFactory(ctrl)
	var servers []*routingServer
	connections := map[string]*grpc.ClientConn{}
	for i, fault := range faults {
		endpoint := &routingServer{requests: make(chan proto.Message, 20), fault: fault}
		servers = append(servers, endpoint)
		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer(grpc.UnaryInterceptor(func(
			ctx context.Context,
			req any,
			_ *grpc.UnaryServerInfo,
			h grpc.UnaryHandler,
		) (any, error) {
			resp, err := h(ctx, req)
			return resp, serviceerror.ToStatus(err).Err()
		}))
		historydlqpb.RegisterHistoryDLQServiceServer(server, endpoint)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		t.Cleanup(func() { _ = listener.Close() })
		connection, err := grpc.NewClient(
			"passthrough:///"+fmt.Sprint(i),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(func(
				ctx context.Context,
				method string,
				req, resp any,
				cc *grpc.ClientConn,
				invoker grpc.UnaryInvoker,
				opts ...grpc.CallOption,
			) error {
				return serviceerrors.FromStatus(status.Convert(invoker(ctx, method, req, resp, cc, opts...)))
			}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		connections[fmt.Sprint("host", i)] = connection
	}
	factory.EXPECT().CreateHistoryGRPCConnection(gomock.Any()).DoAndReturn(func(address string) *grpc.ClientConn {
		return connections[address]
	}).AnyTimes()
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideValue(dynamicconfig.HistoryClientOwnershipCachingEnabled.Key(), caching)
	collection := dynamicconfig.NewCollection(dc, log.NewTestLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	lifecycle := fxtest.NewLifecycle(t)
	lifecycle.RequireStart()
	t.Cleanup(lifecycle.RequireStop)
	metricHandler := metricstest.NewCaptureHandler()
	client, err := historydlqpb.NewHistoryDLQServiceLayeredClient(
		lifecycle,
		collection,
		factory,
		monitor,
		&config.Persistence{
			NumHistoryShards: 32,
		},
		log.NewTestLogger(),
		metricHandler,
	)
	require.NoError(t, err)
	return client, servers, shards, metricHandler
}

func TestLayeredClientRoutesByDLQIdentity(t *testing.T) {
	t.Parallel()
	for _, caching := range []bool{false, true} {
		t.Run(fmt.Sprint(caching), func(t *testing.T) {
			t.Parallel()
			client, servers, shards, mh := newLayeredClientTestEnvironment(t, caching, func() error { return nil })
			capture := mh.StartCapture()
			ref := testMessageRefProto(t)
			requests := []proto.Message{
				&historydlqpb.EnqueueRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					BusinessId:        ref.BusinessId,
				},
				&historydlqpb.ReadMessageRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Ref:               ref,
				},
				&historydlqpb.ResolveRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Ref:               ref,
				},
			}
			_, err := client.Enqueue(t.Context(), requests[0].(*historydlqpb.EnqueueRequest))
			require.NoError(t, err)
			_, err = client.ReadMessage(t.Context(), requests[1].(*historydlqpb.ReadMessageRequest))
			require.NoError(t, err)
			_, err = client.Resolve(t.Context(), requests[2].(*historydlqpb.ResolveRequest))
			require.NoError(t, err)
			require.Equal(
				t,
				fmt.Sprint(common.WorkflowIDToHistoryShard(primitives.SystemNamespaceID, ref.BusinessId, 32)),
				await.Rcv(t, shards),
			)
			for _, req := range requests {
				protorequire.ProtoEqual(t, req, await.Rcv(t, servers[0].requests))
			}
			require.Len(t, capture.Snapshot()[metrics.ClientRequests.Name()], 3)
			require.Len(t, capture.Snapshot()[metrics.ClientLatency.Name()], 3)
			require.Empty(t, capture.Snapshot()[metrics.ClientFailures.Name()])
		})
	}
}

func TestLayeredClientOwnershipChange(t *testing.T) {
	t.Parallel()
	for _, caching := range []bool{false, true} {
		t.Run(fmt.Sprint(caching), func(t *testing.T) {
			t.Parallel()
			client, servers, _, _ := newLayeredClientTestEnvironment(
				t,
				caching,
				func() error { return serviceerrors.NewShardOwnershipLost("host1", "host0") },
				func() error { return nil },
			)
			ref := testMessageRefProto(t)
			req := &historydlqpb.EnqueueRequest{SystemNamespaceId: primitives.SystemNamespaceID, BusinessId: ref.BusinessId}
			_, err := client.Enqueue(t.Context(), req)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req, await.Rcv(t, servers[0].requests))
			protorequire.ProtoEqual(t, req, await.Rcv(t, servers[1].requests))
		})
	}
}

func TestLayeredClientRetriesSameRequest(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	client, servers, _, mh := newLayeredClientTestEnvironment(
		t,
		false,
		func() error {
			if calls.Add(1) == 1 {
				return serviceerror.NewUnavailable("response lost after handler")
			}
			return nil
		},
	)
	capture := mh.StartCapture()
	ref := testMessageRefProto(t)
	req := &historydlqpb.EnqueueRequest{SystemNamespaceId: primitives.SystemNamespaceID, BusinessId: ref.BusinessId}
	_, err := client.Enqueue(t.Context(), req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, req, await.Rcv(t, servers[0].requests))
	protorequire.ProtoEqual(t, req, await.Rcv(t, servers[0].requests))
	require.Len(t, capture.Snapshot()[metrics.ClientFailures.Name()], 1)
}

func TestLayeredClientStopsOnDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	client, _, _, _ := newLayeredClientTestEnvironment(
		t,
		false,
		func() error {
			if calls.Add(1) == 1 {
				entered <- struct{}{}
			}
			select {
			case <-release:
			case <-t.Context().Done():
			}
			return serviceerror.NewUnavailable("injected")
		},
	)
	result := make(chan error, 1)
	go func() {
		_, err := client.Enqueue(ctx, &historydlqpb.EnqueueRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			BusinessId:        "cancel",
		})
		result <- err
	}()
	await.Rcv(t, entered)
	cancel()
	close(release)
	require.Error(t, await.Rcv(t, result))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, int32(1), calls.Load())
}
