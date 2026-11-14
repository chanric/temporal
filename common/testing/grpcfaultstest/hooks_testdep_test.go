//go:build test_dep

package grpcfaultstest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

func TestUnaryServerInterceptor_NoGenerator(t *testing.T) {
	t.Parallel()

	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testhooks.NewTestHooks()))
	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "response", response)
}

func TestUnaryServerInterceptor_BeforeHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	injectedErr := errors.New("injected")
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, any) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Error: injectedErr}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))
	handlerCalled := false

	response, err := interceptor(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return "response", nil
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.False(t, handlerCalled)
}

func TestUnaryServerInterceptor_AfterHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterResponseCallback(grpcfaults.Scope{NamespaceName: "namespace-name"}, func(context.Context, string, any, any, error) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Response: "replacement"}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))

	response, err := interceptor(
		context.Background(),
		&workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "replacement", response)
}

func TestUnaryServerInterceptor_HandlerError(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterResponseCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(_ context.Context, _ string, _, response any, err error) *grpcfaults.Outcome {
		require.Nil(t, response)
		require.ErrorIs(t, err, handlerErr)
		return &grpcfaults.Outcome{Error: injectedErr}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks))
	handlerCalled := false

	response, err := interceptor(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return nil, handlerErr
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.True(t, handlerCalled)
}

func TestNewCallbackGenerator_UnregisterRemovesHook(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := grpcfaultstest.NewCallbackGenerator(testHooks)
	unregister := generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, any) *grpcfaults.Outcome {
		return nil
	})
	_, ok := testhooks.Get(testHooks, testhooks.GRPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)

	unregister()
	_, ok = testhooks.Get(testHooks, testhooks.GRPCRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.False(t, ok)
}

func TestClusterWideFaults(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"request", "response"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			hooks := testhooks.NewTestHooks()
			generator := grpcfaultstest.NewCallbackGenerator(hooks)
			injected := errors.New("injected")
			var unregister func()
			if stage == "request" {
				unregister = generator.RegisterRequestCallback(grpcfaults.Scope{}, func(context.Context, string, any) *grpcfaults.Outcome {
					return &grpcfaults.Outcome{Error: injected}
				})
			} else {
				unregister = generator.RegisterResponseCallback(grpcfaults.Scope{}, func(context.Context, string, any, any, error) *grpcfaults.Outcome {
					return &grpcfaults.Outcome{Error: injected}
				})
			}
			interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(hooks))
			var calls int
			call := func() (any, error) {
				return interceptor(t.Context(), "request without a namespace", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
					func(context.Context, any) (any, error) {
						calls++
						return "response", nil
					})
			}
			_, err := call()
			require.ErrorIs(t, err, injected)
			if stage == "request" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
			unregister()
			response, err := call()
			require.NoError(t, err)
			require.Equal(t, "response", response)
		})
	}
}

func TestNamespaceFaultPrecedesClusterFault(t *testing.T) {
	t.Parallel()
	hooks := testhooks.NewTestHooks()
	generator := grpcfaultstest.NewCallbackGenerator(hooks)
	clusterError, namespaceError := errors.New("cluster"), errors.New("namespace")
	t.Cleanup(generator.RegisterRequestCallback(grpcfaults.Scope{}, func(context.Context, string, any) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Error: clusterError}
	}))
	unregister := generator.RegisterRequestCallback(grpcfaults.Scope{NamespaceID: "namespace-id"},
		func(context.Context, string, any) *grpcfaults.Outcome {
			return &grpcfaults.Outcome{Error: namespaceError}
		})
	serverGenerator := grpcfaultstest.NewGenerator(hooks)
	request := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	outcome := serverGenerator.GenerateRequest(t.Context(), "/test.Service/Method", request)
	require.ErrorIs(t, outcome.Error, namespaceError)
	unregister()
	outcome = serverGenerator.GenerateRequest(t.Context(), "/test.Service/Method", request)
	require.ErrorIs(t, outcome.Error, clusterError)
}
