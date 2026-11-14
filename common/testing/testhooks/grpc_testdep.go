//go:build test_dep

package testhooks

import "go.temporal.io/server/common/rpc/grpcfaults"

// Cluster-wide RPC hooks require a dedicated test cluster.
var (
	GRPCRequestFaultGeneratorGlobal  = newKey[grpcfaults.RequestCallback, global]()
	GRPCResponseFaultGeneratorGlobal = newKey[grpcfaults.ResponseCallback, global]()
)
