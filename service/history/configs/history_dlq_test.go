package configs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func TestHistoryTaskDLQUseCHASMDefault(t *testing.T) {
	t.Parallel()
	dc := dynamicconfig.NewMemoryClient()
	collection := dynamicconfig.NewCollection(dc, log.NewTestLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	cfg := NewConfig(collection, 32)
	require.Equal(
		t,
		dynamicconfig.MakeKey("history.TaskDLQUseCHASM"),
		dynamicconfig.HistoryTaskDLQUseCHASM.Key(),
	)
	require.False(t, cfg.TaskDLQUseCHASM())
	cleanup := dc.OverrideSetting(dynamicconfig.HistoryTaskDLQUseCHASM, true)
	require.True(t, cfg.TaskDLQUseCHASM())
	cleanup()
	require.False(t, cfg.TaskDLQUseCHASM())
}
