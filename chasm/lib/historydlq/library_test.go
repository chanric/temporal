package historydlq

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLibraryRegistersHistoryDLQService(t *testing.T) {
	t.Parallel()
	handler, _, _, _ := newHandlerTestEnvironment(t)
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	newLibrary(handler).RegisterServices(server)
	info, ok := server.GetServiceInfo()["temporal.server.chasm.lib.historydlq.proto.v1.HistoryDLQService"]
	require.True(t, ok)
	require.Len(t, info.Methods, 6)
}

func TestNilLibraryHasNoService(t *testing.T) {
	t.Parallel()
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	NewNilLibrary().RegisterServices(server)
	require.Empty(t, server.GetServiceInfo())
}
