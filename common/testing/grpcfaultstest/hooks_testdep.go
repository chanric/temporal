//go:build test_dep

package grpcfaultstest

import (
	"context"

	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// NewCallbackGenerator creates a CallbackGenerator connected to test hooks.
// An empty scope requires a dedicated test cluster.
func NewCallbackGenerator(testHooks commontesthooks.TestHooks) *grpcfaults.CallbackGenerator {
	return grpcfaults.NewCallbackGeneratorWithHooks(newAdapter(testHooks))
}

// NewGenerator creates a Generator backed by test hooks.
func NewGenerator(testHooks commontesthooks.TestHooks) grpcfaults.Generator {
	return newAdapter(testHooks)
}

type adapter struct {
	faultinjectiontest.Adapter[any, any]
	testHooks commontesthooks.TestHooks
}

func newAdapter(testHooks commontesthooks.TestHooks) adapter {
	return adapter{
		testHooks: testHooks,
		Adapter: faultinjectiontest.NewAdapter(testHooks, faultinjectiontest.HookKeys[any, any]{
			Request: faultinjectiontest.Keys[grpcfaults.RequestCallback]{
				ByNamespaceID:   commontesthooks.GRPCRequestFaultGeneratorByNamespaceID,
				ByNamespaceName: commontesthooks.GRPCRequestFaultGeneratorByNamespaceName,
			},
			Response: faultinjectiontest.Keys[grpcfaults.ResponseCallback]{
				ByNamespaceID:   commontesthooks.GRPCResponseFaultGeneratorByNamespaceID,
				ByNamespaceName: commontesthooks.GRPCResponseFaultGeneratorByNamespaceName,
			},
		}),
	}
}

func (a adapter) InstallRequestCallback(scope grpcfaults.Scope, callback grpcfaults.RequestCallback) func() {
	if scope.NamespaceID == "" && scope.NamespaceName == "" {
		return commontesthooks.Set(a.testHooks, commontesthooks.GRPCRequestFaultGeneratorGlobal, callback, commontesthooks.GlobalScope)
	}
	return a.Adapter.InstallRequestCallback(scope, callback)
}

func (a adapter) InstallResponseCallback(scope grpcfaults.Scope, callback grpcfaults.ResponseCallback) func() {
	if scope.NamespaceID == "" && scope.NamespaceName == "" {
		return commontesthooks.Set(a.testHooks, commontesthooks.GRPCResponseFaultGeneratorGlobal, callback, commontesthooks.GlobalScope)
	}
	return a.Adapter.InstallResponseCallback(scope, callback)
}

func (a adapter) GenerateRequest(ctx context.Context, operation string, request any) *grpcfaults.Outcome {
	if outcome := a.Adapter.GenerateRequest(ctx, operation, request); outcome != nil {
		return outcome
	}
	if callback, ok := commontesthooks.Get(a.testHooks, commontesthooks.GRPCRequestFaultGeneratorGlobal, commontesthooks.GlobalScope); ok {
		return callback(ctx, operation, request)
	}
	return nil
}

func (a adapter) GenerateResponse(ctx context.Context, operation string, request, response any, err error) *grpcfaults.Outcome {
	if outcome := a.Adapter.GenerateResponse(ctx, operation, request, response, err); outcome != nil {
		return outcome
	}
	if callback, ok := commontesthooks.Get(a.testHooks, commontesthooks.GRPCResponseFaultGeneratorGlobal, commontesthooks.GlobalScope); ok {
		return callback(ctx, operation, request, response, err)
	}
	return nil
}
