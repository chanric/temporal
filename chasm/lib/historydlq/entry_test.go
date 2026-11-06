package historydlq

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
)

func TestEntryReadMissingTask(t *testing.T) {
	t.Parallel()
	entry := &Entry{EntryState: &historydlqpb.EntryState{Status: historydlqpb.MESSAGE_STATUS_PENDING}}
	_, err := entry.Read(&chasm.MockContext{}, MessageRef{})
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}

func TestEntryReadResolvedWithoutTask(t *testing.T) {
	t.Parallel()
	entry := &Entry{EntryState: &historydlqpb.EntryState{
		Status: historydlqpb.MESSAGE_STATUS_RESOLVED, Resolution: historydlqpb.RESOLUTION_PURGED,
	}}
	stored, err := entry.Read(&chasm.MockContext{}, MessageRef{})
	require.NoError(t, err)
	require.Equal(t, StoredMessage{Status: historydlqpb.MESSAGE_STATUS_RESOLVED, Resolution: historydlqpb.RESOLUTION_PURGED}, stored)
}
