package dlq

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/encoding/protojson"
)

type replayRegistry struct {
	sdkworker.Registry
	replayer sdkworker.WorkflowReplayer
}

func (r replayRegistry) RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions) {
	r.replayer.RegisterWorkflowWithOptions(w, options)
}

func TestLegacyWorkflowReplay(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{WorkflowTypeDelete, WorkflowTypeMerge} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			// Fixtures model the QueueV2 command sequence before mixed-store jobs.
			data, err := os.ReadFile("testdata/legacy_" + operation + ".json")
			require.NoError(t, err)
			var history historypb.History
			require.NoError(t, protojson.Unmarshal(data, &history))
			replayer := sdkworker.NewWorkflowReplayer()
			(&workerComponent{}).RegisterWorkflow(replayRegistry{replayer: replayer})
			require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), &history))
		})
	}
}
