package historydlq

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
)

func TestEnqueueSegmentWarningMetrics(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	mh := metricstest.NewCaptureHandler()
	h.metricsHandler = mh
	capture := mh.StartCapture()
	t.Cleanup(func() { mh.StopCapture(capture) })

	req := testEnqueueRequestProto(t)
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 1<<20)
	_, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Empty(t, capture.SnapshotMetric("history_dlq_segment_warning_threshold_crossed"))

	req.SourceTaskIdentity.TaskId++
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 2<<20)
	large, err := h.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Len(t, capture.SnapshotMetric("history_dlq_segment_warning_threshold_crossed"), 1)
	_, err = h.Enqueue(ctx, req)
	require.NoError(t, err)
	req.SourceTaskIdentity.TaskId++
	req.Task.Blob.Data = []byte("above threshold")
	_, err = h.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Len(t, capture.SnapshotMetric("history_dlq_segment_warning_threshold_crossed"), 1)

	_, err = h.Resolve(ctx, &historydlqpb.ResolveRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               large.Ref,
		Resolution:        historydlqpb.RESOLUTION_PURGED,
	})
	require.NoError(t, err)
	req.SourceTaskIdentity.TaskId++
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 2<<20)
	_, err = h.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Equal(t, []*metricstest.CapturedRecording{
		{Value: int64(1), Tags: map[string]string{"task_category": "1", "source_cluster": "source", "target_cluster": "target"}},
		{Value: int64(1), Tags: map[string]string{"task_category": "1", "source_cluster": "source", "target_cluster": "target"}},
	}, capture.SnapshotMetric("history_dlq_segment_warning_threshold_crossed"))
	require.Empty(t, capture.SnapshotMetric("history_dlq_segment_budget_exceeded"))
}

func TestEnqueueSegmentBudgetMetrics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*historydlqpb.EnqueueRequest)
	}{
		{"payload", func(req *historydlqpb.EnqueueRequest) { req.Task.Blob.Data = bytes.Repeat([]byte{42}, 4<<20) }},
		{"entry metadata", func(req *historydlqpb.EnqueueRequest) {
			req.SourceTaskIdentity.BusinessId = string(bytes.Repeat([]byte{42}, 4<<20))
		}},
		{"segment metadata", func(req *historydlqpb.EnqueueRequest) {
			req.Queue.SourceCluster = string(bytes.Repeat([]byte{42}, 4<<20))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, _, _ := newHandlerTestEnvironment(t)
			mh := metricstest.NewCaptureHandler()
			h.metricsHandler = mh
			capture := mh.StartCapture()
			t.Cleanup(func() { mh.StopCapture(capture) })
			req := testEnqueueRequestProto(t)
			tc.change(req)
			var err error
			req.BusinessId, err = SegmentBusinessID(queueProtoToKey(req.Queue), taskIdentityProtoToIdentity(req.SourceTaskIdentity))
			require.NoError(t, err)
			for range 2 {
				response, err := h.Enqueue(ctx, req)
				require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
				require.Nil(t, response)
			}
			require.Equal(t, []*metricstest.CapturedRecording{
				{Value: int64(1), Tags: map[string]string{"task_category": "1", "source_cluster": req.Queue.SourceCluster, "target_cluster": "target"}},
				{Value: int64(1), Tags: map[string]string{"task_category": "1", "source_cluster": req.Queue.SourceCluster, "target_cluster": "target"}},
			}, capture.SnapshotMetric("history_dlq_segment_budget_exceeded"))
			require.Empty(t, capture.SnapshotMetric("history_dlq_segment_warning_threshold_crossed"))
		})
	}
}

func TestEnqueueSegmentMetricsOnFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		engine controlledEngine
	}{
		{"engine unavailable", controlledEngine{updateError: serviceerror.NewUnavailable("unavailable")}},
		{"unrelated resource exhaustion", controlledEngine{updateError: serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "persistence overloaded")}},
		{"failure after append", controlledEngine{beforeCommitError: serviceerror.NewUnavailable("persistence failed")}},
		{"unaccepted result", controlledEngine{mismatch: true}},
		{"lost response", controlledEngine{lostResponse: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _, engine, _ := newHandlerTestEnvironment(t)
			mh := metricstest.NewCaptureHandler()
			h.metricsHandler = mh
			capture := mh.StartCapture()
			t.Cleanup(func() { mh.StopCapture(capture) })
			req := testEnqueueRequestProto(t)
			req.Task.Blob.Data = bytes.Repeat([]byte{42}, 3<<20)
			tc.engine.Engine = engine
			response, err := h.Enqueue(chasm.NewEngineContext(t.Context(), &tc.engine), req)
			require.Error(t, err)
			require.Nil(t, response)
			require.Empty(t, capture.SnapshotMetric("history_dlq_segment_warning_threshold_crossed"))
			require.Empty(t, capture.SnapshotMetric("history_dlq_segment_budget_exceeded"))
		})
	}
}
