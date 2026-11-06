package historydlq

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSegmentNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		namespaceID string
	}{
		{"empty", ""},
		{"user namespace", "user-namespace"},
		{"namespace name instead of ID", "temporal-system"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			key := ctx.ExecutionKey()
			key.NamespaceID = tc.namespaceID
			ctx.HandleExecutionKey = func() chasm.ExecutionKey { return key }
			segment, err := NewSegment(ctx, componentRequest())
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.Nil(t, segment)
		})
	}
}

func TestSegmentNamespaceAccess(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"append", "duplicate", "read", "resolve"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := componentRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			ref, err := segment.Append(ctx, req)
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			wrongCtx := newSegmentContext(&now)
			key := ctx.ExecutionKey()
			key.NamespaceID = "user-namespace"
			wrongCtx.HandleExecutionKey = func() chasm.ExecutionKey { return key }
			switch operation {
			case "append", "duplicate":
				if operation == "append" {
					req.SourceTaskIdentity.TaskID++
				}
				_, err = segment.Append(wrongCtx, req)
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			case "read":
				_, err = segment.ReadEntry(wrongCtx, ref)
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			case "resolve":
				err = segment.ResolveEntry(wrongCtx, ref, historydlqpb.RESOLUTION_PURGED)
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				t.Fatalf("unknown operation %q", operation)
			}
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			require.Len(t, segment.Entries, 1)
			stored, err := segment.ReadEntry(ctx, ref)
			require.NoError(t, err)
			require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, stored.Status)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
		})
	}
}

func TestSegmentQueueIdentity(t *testing.T) {
	t.Parallel()
	req := componentRequest()
	segmentID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	retryID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, segmentID, retryID)

	changedOriginal := req
	changedOriginal.SourceTaskIdentity.TaskID++
	unchangedID, err := SegmentBusinessID(changedOriginal.Queue, changedOriginal.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, segmentID, unchangedID)

	changedQueue := req.Queue
	changedQueue.TargetCluster = "other-target"
	otherID, err := SegmentBusinessID(changedQueue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.NotEqual(t, segmentID, otherID)

	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	require.Equal(t, req.Queue.CategoryID, segment.GetQueue().GetTaskCategory())
	require.Equal(t, req.Queue.SourceCluster, segment.GetQueue().GetSourceCluster())
	require.Equal(t, req.Queue.TargetCluster, segment.GetQueue().GetTargetCluster())
	require.Empty(t, segment.Entries)
	require.Equal(t, int32(7), segment.GetSourceShardId())
	require.Equal(t, int64(1), segment.GetTaskIdRangeStart())
	require.Equal(t, []chasm.SearchAttributeKeyValue{
		categorySearchAttribute.Value(int64(req.Queue.CategoryID)),
		sourceSearchAttribute.Value(req.Queue.SourceCluster),
		targetSearchAttribute.Value(req.Queue.TargetCluster),
		pendingSearchAttribute.Value(false),
	}, segment.SearchAttributes(ctx))
	require.IsType(t, &emptypb.Empty{}, segment.Memo(ctx))
}

func TestSegmentAppendDistinctEntries(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 123, time.UTC)
	ctx := newSegmentContext(&now)
	first := componentRequest()
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRef, err := segment.Append(ctx, first)
	require.NoError(t, err)
	second := componentRequest()
	second.SourceTaskIdentity.TaskID++
	second.Task = proto.Clone(second.Task).(*commonspb.HistoryTask)
	secondRef, err := segment.Append(ctx, second)
	require.NoError(t, err)

	require.NotEqual(t, firstRef.EntryKey, secondRef.EntryKey)
	require.Equal(t, firstRef.BusinessID, secondRef.BusinessID)
	require.Equal(t, firstRef.RunID, secondRef.RunID)
	require.Equal(t, first.Queue, firstRef.Queue)
	require.Equal(t, second.Queue, secondRef.Queue)
	require.Len(t, segment.Entries, 2)
	require.Equal(t, pendingSearchAttribute.Value(true), segment.SearchAttributes(ctx)[3])

	stored, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	require.Equal(t, secondRef, stored.Ref)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, stored.Status)
	protorequire.ProtoEqual(t, second.Task, stored.Task)
	second.Task.Blob.Data[0] = 'x'
	stored.Task.Blob.Data[0] = 'y'
	again, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	require.Equal(t, []byte("task payload"), again.Task.Blob.Data)
}

func TestSegmentAppendDuplicate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := componentRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	first, err := segment.Append(ctx, req)
	require.NoError(t, err)
	now = now.Add(time.Hour)
	duplicate, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.Equal(t, first, duplicate)
	require.Len(t, segment.Entries, 1)
}

func TestSegmentRetryAfterResolution(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := componentRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	accepted, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.NoError(t, segment.ResolveEntry(ctx, accepted, historydlqpb.RESOLUTION_MERGED))
	now = now.Add(time.Hour)
	retry, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.Equal(t, accepted, retry)
	stored, err := segment.ReadEntry(ctx, retry)
	require.NoError(t, err)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_RESOLVED, stored.Status)
	require.Equal(t, historydlqpb.RESOLUTION_MERGED, stored.Resolution)
}

func TestSegmentResolveOneEntry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	first := componentRequest()
	second := componentRequest()
	second.SourceTaskIdentity.TaskID++
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRef, err := segment.Append(ctx, first)
	require.NoError(t, err)
	secondRef, err := segment.Append(ctx, second)
	require.NoError(t, err)

	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_PURGED))
	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_MERGED))
	resolved, err := segment.ReadEntry(ctx, firstRef)
	require.NoError(t, err)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_RESOLVED, resolved.Status)
	require.Equal(t, historydlqpb.RESOLUTION_PURGED, resolved.Resolution)
	pending, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, pending.Status)
}

func TestSegmentReferenceValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		change   func(*MessageRef)
		notFound bool
	}{
		{"business ID", func(ref *MessageRef) { ref.BusinessID = "other-segment" }, true},
		{"run ID", func(ref *MessageRef) { ref.RunID = "replacement-run" }, true},
		{"entry key", func(ref *MessageRef) { ref.EntryKey = "other-entry" }, true},
		{"queue", func(ref *MessageRef) { ref.Queue.TargetCluster = "other-target" }, false},
		{"message ID", func(ref *MessageRef) { ref.MessageID++ }, false},
		{"enqueue time", func(ref *MessageRef) { ref.EnqueuedAt = ref.EnqueuedAt.Add(time.Nanosecond) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := componentRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			ref, err := segment.Append(ctx, req)
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			invalid := ref
			tc.change(&invalid)
			_, readErr := segment.ReadEntry(ctx, invalid)
			resolveErr := segment.ResolveEntry(ctx, invalid, historydlqpb.RESOLUTION_PURGED)
			for _, err := range []error{readErr, resolveErr} {
				if tc.notFound {
					require.ErrorAs(t, err, new(*serviceerror.NotFound))
				} else {
					require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
				}
			}
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			stored, err := segment.ReadEntry(ctx, ref)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			stored.Task = nil
			require.Equal(t, StoredMessage{Ref: ref, SourceTaskIdentity: req.SourceTaskIdentity, Status: historydlqpb.MESSAGE_STATUS_PENDING}, stored)
		})
	}
}

func TestSegmentPersistence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 123, time.UTC)
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	backend := &chasm.MockNodeBackend{
		HandleNow: func() time.Time { return now },
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(primitives.SystemNamespaceID, "segment-business", "segment-run")
		},
		HandleGetExecutionInfo:    func() *persistencespb.WorkflowExecutionInfo { return &persistencespb.WorkflowExecutionInfo{} },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	root := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(t.Context(), root)
	req := componentRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	admissionTime := now
	now = now.Add(time.Second)
	ref, err := segment.Append(ctx, req)
	require.NoError(t, err)
	wantID, err := messageID(req.Queue, req.SourceTaskIdentity, admissionTime)
	require.NoError(t, err)
	require.Equal(t, admissionTime, ref.EnqueuedAt)
	require.Equal(t, wantID, ref.MessageID)
	require.NoError(t, root.SetRootComponent(segment))
	reload := func() chasm.NodesMutation {
		wantState := proto.Clone(segment.SegmentState)
		mutation, err := root.CloseTransaction(t.Context(), true)
		require.NoError(t, err)
		nodes := root.Snapshot(nil).Nodes
		for path, node := range nodes {
			nodes[path] = proto.Clone(node).(*persistencespb.ChasmNode)
		}
		root, err = chasm.NewTreeFromDB(nodes, registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
		require.NoError(t, err)
		now = now.Add(time.Hour)
		ctx = chasm.NewMutableContext(t.Context(), root)
		component, err := root.Component(ctx, chasm.ComponentRef{})
		require.NoError(t, err)
		segment = component.(*Segment)
		protorequire.ProtoEqual(t, wantState, segment.SegmentState)
		return mutation
	}
	checkStored := func(want StoredMessage) {
		stored, err := segment.ReadEntry(ctx, want.Ref)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, want.Task, stored.Task)
		want.Task, stored.Task = nil, nil
		require.Equal(t, want, stored)
	}
	reload()
	checkStored(StoredMessage{Ref: ref, SourceTaskIdentity: req.SourceTaskIdentity, Status: historydlqpb.MESSAGE_STATUS_PENDING, Task: req.Task})
	duplicate, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.Equal(t, ref, duplicate)
	second := componentRequest()
	second.SourceTaskIdentity.TaskID++
	secondRef, err := segment.Append(ctx, second)
	require.NoError(t, err)
	resolvedAt := ctx.Now(segment)
	require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_PURGED))
	mutation := reload()
	require.Len(t, mutation.DeletedNodes, 1, "resolution must delete the task DATA node")
	checkStored(StoredMessage{Ref: ref, SourceTaskIdentity: req.SourceTaskIdentity, Status: historydlqpb.MESSAGE_STATUS_RESOLVED, Resolution: historydlqpb.RESOLUTION_PURGED})
	checkStored(StoredMessage{Ref: secondRef, SourceTaskIdentity: second.SourceTaskIdentity, Status: historydlqpb.MESSAGE_STATUS_PENDING, Task: second.Task})
	duplicate, err = segment.Append(ctx, req)
	require.NoError(t, err)
	require.Equal(t, ref, duplicate)
	require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_MERGED))
	reload()
	entry, err := segment.entry(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, resolvedAt, entry.GetResolvedAt().AsTime())
	require.Equal(t, chasm.LifecycleStateCompleted, entry.LifecycleState(ctx))
	require.Equal(t, chasm.LifecycleStateRunning, segment.LifecycleState(ctx))
	checkStored(StoredMessage{Ref: ref, SourceTaskIdentity: req.SourceTaskIdentity, Status: historydlqpb.MESSAGE_STATUS_RESOLVED, Resolution: historydlqpb.RESOLUTION_PURGED})
}

func TestSegmentResolveReclaimsTask(t *testing.T) {
	t.Parallel()
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
		t.Run(reason.String(), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := componentRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			ref, err := segment.Append(ctx, req)
			require.NoError(t, err)
			entry, err := segment.entry(ctx, ref)
			require.NoError(t, err)
			require.Equal(t, chasm.LifecycleStateRunning, entry.LifecycleState(ctx))
			require.NoError(t, segment.ResolveEntry(ctx, ref, reason))
			require.Equal(t, chasm.LifecycleStateCompleted, entry.LifecycleState(ctx))
			require.Equal(t, chasm.LifecycleStateRunning, segment.LifecycleState(ctx))
			_, ok := entry.Task.TryGet(ctx)
			require.False(t, ok)
			stored, err := segment.ReadEntry(ctx, ref)
			require.NoError(t, err)
			require.Equal(t, StoredMessage{Ref: ref, SourceTaskIdentity: req.SourceTaskIdentity, Status: historydlqpb.MESSAGE_STATUS_RESOLVED, Resolution: reason}, stored)
		})
	}
}

func TestMessageRefEntryKeyWireNumber(t *testing.T) {
	t.Parallel()
	field := (&historydlqpb.MessageRef{}).ProtoReflect().Descriptor().Fields().ByName("entry_key")
	require.NotNil(t, field)
	require.Equal(t, protoreflect.FieldNumber(7), field.Number())
}

func TestSegmentNumericIDCollision(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	first := componentRequest()
	first.SourceTaskIdentity.TaskID = 7115
	second := componentRequest()
	second.SourceTaskIdentity.TaskID = 7134
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRef, err := segment.Append(ctx, first)
	require.NoError(t, err)
	secondRef, err := segment.Append(ctx, second)
	require.NoError(t, err)

	require.Equal(t, firstRef.MessageID, secondRef.MessageID)
	require.NotEqual(t, firstRef.EntryKey, secondRef.EntryKey)
	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_PURGED))
	firstStored, err := segment.ReadEntry(ctx, firstRef)
	require.NoError(t, err)
	secondStored, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_RESOLVED, firstStored.Status)
	require.Equal(t, historydlqpb.MESSAGE_STATUS_PENDING, secondStored.Status)
}

func TestSegmentFixedRange(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := componentRequest()
	req.SourceTaskIdentity.TaskID = 1
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	var firstRef MessageRef
	for taskID := int64(1); taskID <= 128; taskID++ {
		req.SourceTaskIdentity.TaskID = taskID
		ref, err := segment.Append(ctx, req)
		require.NoError(t, err)
		if taskID == 1 {
			firstRef = ref
		}
	}
	require.Len(t, segment.Entries, 128)
	req.SourceTaskIdentity.TaskID = 129
	_, err = segment.Append(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	req.SourceTaskIdentity.TaskID = 1
	req.SourceTaskIdentity.SourceShardID++
	_, err = segment.Append(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	req.SourceTaskIdentity.SourceShardID--
	duplicate, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.Equal(t, firstRef, duplicate)
	require.Len(t, segment.Entries, 128)
}

func TestSegmentByteBudget(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	first := componentRequest()
	first.Task.Blob.Data = bytes.Repeat([]byte{42}, 3<<20)
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRef, err := segment.Append(ctx, first)
	require.NoError(t, err)
	before := proto.Clone(segment.SegmentState)
	second := componentRequest()
	second.SourceTaskIdentity.TaskID++
	second.Task.Blob.Data = bytes.Repeat([]byte{42}, 2<<20)
	_, err = segment.Append(ctx, second)
	require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
	protorequire.ProtoEqual(t, before, segment.SegmentState)
	require.Len(t, segment.Entries, 1)
	duplicate, err := segment.Append(ctx, first)
	require.NoError(t, err)
	require.Equal(t, firstRef, duplicate)
	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_PURGED))
	secondRef, err := segment.Append(ctx, second)
	require.NoError(t, err)
	stored, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, second.Task, stored.Task)
	require.LessOrEqual(t, segment.GetRetainedBytes(), int64(4<<20))
}

func TestSegmentOversizedEntry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		metadata bool
	}{
		{"payload", false},
		{"metadata", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := componentRequest()
			if tc.metadata {
				req.SourceTaskIdentity.BusinessID = string(bytes.Repeat([]byte{42}, 4<<20))
			} else {
				req.Task.Blob.Data = bytes.Repeat([]byte{42}, 4<<20)
			}
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			_, err = segment.Append(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			require.Empty(t, segment.Entries)
		})
	}
}

func newSegmentContext(now *time.Time) *chasm.MockMutableContext {
	return &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return *now },
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{NamespaceID: primitives.SystemNamespaceID, BusinessID: "segment-business", RunID: "segment-run"}
		},
	}}
}
