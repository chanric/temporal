package historydlq

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Reserve headroom under the default 8 MiB mutable-state limit.
	segmentRetainedBytesLimit int64 = 4 << 20
	// Covers framework node metadata, paths, and resolution metadata growth.
	segmentNodeReserve int64 = 1024
)

type Segment struct {
	chasm.UnimplementedComponent
	*historydlqpb.SegmentState
	Entries    chasm.Map[string, *Entry]
	Visibility chasm.Field[*chasm.Visibility]
}

var (
	_ chasm.RootComponent                      = (*Segment)(nil)
	_ chasm.VisibilitySearchAttributesProvider = (*Segment)(nil)
	_ chasm.VisibilityMemoProvider             = (*Segment)(nil)
)

func NewSegment(ctx chasm.MutableContext, req EnqueueRequest) (*Segment, error) {
	if ctx.ExecutionKey().NamespaceID != primitives.SystemNamespaceID {
		return nil, serviceerror.NewInvalidArgument("history DLQ segments require the temporal-system namespace")
	}
	if err := validateIdentity(req.Queue, req.SourceTaskIdentity); err != nil {
		return nil, err
	}
	state := &historydlqpb.SegmentState{
		Queue:            queueProto(req.Queue),
		SourceShardId:    req.SourceTaskIdentity.SourceShardID,
		TaskIdRangeStart: taskIDRangeStart(req.SourceTaskIdentity.TaskID),
	}
	state.RetainedBytes = int64(proto.Size(state)) + segmentNodeReserve
	if state.RetainedBytes > segmentRetainedBytesLimit {
		return nil, segmentBudgetExceededErr()
	}
	return &Segment{
		SegmentState: state,
		Entries:      make(chasm.Map[string, *Entry]),
		Visibility:   chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}, nil
}

func (s *Segment) Append(ctx chasm.MutableContext, req EnqueueRequest) (MessageRef, error) {
	if ctx.ExecutionKey().NamespaceID != primitives.SystemNamespaceID {
		return MessageRef{}, serviceerror.NewInvalidArgument("history DLQ segments require the temporal-system namespace")
	}
	if queueKey(s.GetQueue()) != req.Queue || req.SourceTaskIdentity.SourceShardID != s.GetSourceShardId() ||
		req.SourceTaskIdentity.TaskID < s.GetTaskIdRangeStart() ||
		req.SourceTaskIdentity.TaskID-s.GetTaskIdRangeStart() >= segmentTaskIDRangeSize {
		return MessageRef{}, serviceerror.NewInvalidArgument("history DLQ entry does not belong to this segment")
	}
	entryKey, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	if err != nil {
		return MessageRef{}, err
	}
	if entryField, ok := s.Entries[entryKey]; ok {
		entry, ok := entryField.TryGet(ctx)
		if !ok {
			return MessageRef{}, serviceerror.NewDataLoss("history DLQ segment entry is missing")
		}
		return s.messageReference(ctx, entryKey, entry.EntryState), nil
	}
	if int64(proto.Size(req.Task)) > segmentRetainedBytesLimit-s.GetRetainedBytes() {
		return MessageRef{}, segmentBudgetExceededErr()
	}
	entry, err := newEntry(ctx, req, ctx.Now(s))
	if err != nil {
		return MessageRef{}, err
	}
	entryBytes := int64(proto.Size(entry.EntryState)+len(entryKey)) + entry.GetPayloadSizeBytes() + segmentNodeReserve
	if entryBytes > segmentRetainedBytesLimit-s.GetRetainedBytes() {
		return MessageRef{}, segmentBudgetExceededErr()
	}
	s.Entries[entryKey] = chasm.NewComponentField(ctx, entry)
	s.RetainedBytes += entryBytes
	return s.messageReference(ctx, entryKey, entry.EntryState), nil
}

func (s *Segment) ReadEntry(ctx chasm.Context, ref MessageRef) (StoredMessage, error) {
	entry, err := s.entry(ctx, ref)
	if err != nil {
		return StoredMessage{}, err
	}
	return entry.Read(ctx, ref)
}

func (s *Segment) ResolveEntry(ctx chasm.MutableContext, ref MessageRef, reason historydlqpb.Resolution) error {
	if reason != historydlqpb.RESOLUTION_MERGED && reason != historydlqpb.RESOLUTION_PURGED {
		return serviceerror.NewInvalidArgument("invalid history DLQ resolution")
	}
	entry, err := s.entry(ctx, ref)
	if err != nil {
		return err
	}
	switch entry.GetStatus() {
	case historydlqpb.MESSAGE_STATUS_RESOLVED:
		return nil
	case historydlqpb.MESSAGE_STATUS_PENDING:
		s.RetainedBytes -= entry.GetPayloadSizeBytes()
		entry.Status = historydlqpb.MESSAGE_STATUS_RESOLVED
		entry.Resolution = reason
		entry.ResolvedAt = timestamppb.New(ctx.Now(s))
		entry.Task = chasm.NewEmptyField[*commonspb.HistoryTask]()
		return nil
	default:
		return serviceerror.NewFailedPrecondition("history DLQ entry has invalid pending state")
	}
}

func segmentBudgetExceededErr() error {
	return &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "history DLQ segment storage budget exceeded",
	}
}

func (s *Segment) LifecycleState(chasm.Context) chasm.LifecycleState {
	// CONSIDER(chanric). close it on pruge
	return chasm.LifecycleStateRunning
}

func (s *Segment) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("history DLQ segments do not support termination")
}

func (s *Segment) ContextMetadata(chasm.Context) map[string]string { return nil }

func (s *Segment) SearchAttributes(ctx chasm.Context) []chasm.SearchAttributeKeyValue {
	queue := s.GetQueue()
	return []chasm.SearchAttributeKeyValue{
		categorySearchAttribute.Value(int64(queue.GetTaskCategory())),
		sourceSearchAttribute.Value(queue.GetSourceCluster()),
		targetSearchAttribute.Value(queue.GetTargetCluster()),
		pendingSearchAttribute.Value(s.hasPending(ctx)),
	}
}

func (s *Segment) Memo(chasm.Context) proto.Message { return &emptypb.Empty{} }

func (s *Segment) hasPending(ctx chasm.Context) bool {
	for _, entryField := range s.Entries {
		entry, ok := entryField.TryGet(ctx)
		if ok && entry.GetStatus() == historydlqpb.MESSAGE_STATUS_PENDING {
			return true
		}
	}
	return false
}

func (s *Segment) entry(ctx chasm.Context, ref MessageRef) (*Entry, error) {
	key := ctx.ExecutionKey()
	if key.NamespaceID != primitives.SystemNamespaceID || ref.BusinessID != key.BusinessID || ref.RunID != key.RunID {
		return nil, serviceerror.NewNotFound("history DLQ segment entry not found")
	}
	entryField, ok := s.Entries[ref.EntryKey]
	if !ok {
		return nil, serviceerror.NewNotFound("history DLQ segment entry not found")
	}
	entry, ok := entryField.TryGet(ctx)
	if !ok {
		return nil, serviceerror.NewDataLoss("history DLQ segment entry is missing")
	}
	if !sameEntryReference(s.messageReference(ctx, ref.EntryKey, entry.EntryState), ref) {
		return nil, serviceerror.NewFailedPrecondition("history DLQ reference does not match the stored entry")
	}
	return entry, nil
}

func (s *Segment) messageReference(ctx chasm.Context, entryKey string, state *historydlqpb.EntryState) MessageRef {
	key := ctx.ExecutionKey()
	return MessageRef{
		BusinessID: key.BusinessID,
		RunID:      key.RunID,
		MessageID:  state.GetMessageId(),
		Queue:      queueKey(s.GetQueue()),
		EnqueuedAt: state.GetEnqueuedAt().AsTime(),
		EntryKey:   entryKey,
	}
}

func queueProto(queue QueueKey) *commonspb.HistoryDLQKey {
	return &commonspb.HistoryDLQKey{TaskCategory: queue.CategoryID, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster}
}

func queueKey(queue *commonspb.HistoryDLQKey) QueueKey {
	return QueueKey{CategoryID: queue.GetTaskCategory(), SourceCluster: queue.GetSourceCluster(), TargetCluster: queue.GetTargetCluster()}
}

func taskIdentityProto(identity TaskIdentity) *historydlqpb.TaskIdentity {
	return &historydlqpb.TaskIdentity{SourceShardId: identity.SourceShardID, TaskId: identity.TaskID, NamespaceId: identity.NamespaceID,
		BusinessId: identity.BusinessID, RunId: identity.RunID, ArchetypeId: identity.ArchetypeID}
}

func taskIdentity(identity *historydlqpb.TaskIdentity) TaskIdentity {
	return TaskIdentity{SourceShardID: identity.GetSourceShardId(), TaskID: identity.GetTaskId(), NamespaceID: identity.GetNamespaceId(),
		BusinessID: identity.GetBusinessId(), RunID: identity.GetRunId(), ArchetypeID: identity.GetArchetypeId()}
}

func sameEntryReference(a, b MessageRef) bool {
	return a.BusinessID == b.BusinessID && a.RunID == b.RunID &&
		a.MessageID == b.MessageID && a.Queue == b.Queue && a.EnqueuedAt.Equal(b.EnqueuedAt) && a.EntryKey == b.EntryKey
}
