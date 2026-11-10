package worker

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/log"
)

func TestHistoryDLQLibraryRegistration(t *testing.T) {
	t.Parallel()
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registerHistoryDLQLibrary(registry))
	archetypeID, registered := registry.ArchetypeIDOf(reflect.TypeFor[*historydlq.Segment]())
	require.True(t, registered)
	_, registered = registry.ComponentByID(archetypeID)
	require.True(t, registered)
}
