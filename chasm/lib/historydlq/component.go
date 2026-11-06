package historydlq

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StoredMessage struct {
	Ref                MessageRef
	SourceTaskIdentity TaskIdentity
	Status             historydlqpb.MessageStatus
	Resolution         historydlqpb.Resolution
	Task               *commonspb.HistoryTask
}

type Message struct {
	chasm.UnimplementedComponent
	*historydlqpb.MessageState
	Task       chasm.Field[*commonspb.HistoryTask]
	Visibility chasm.Field[*chasm.Visibility]
}

var (
	_                         chasm.RootComponent                      = (*Message)(nil)
	_                         chasm.VisibilitySearchAttributesProvider = (*Message)(nil)
	_                         chasm.VisibilityMemoProvider             = (*Message)(nil)
	categorySearchAttribute                                            = chasm.NewSearchAttributeInt("HistoryDLQCategory", chasm.SearchAttributeFieldInt01)
	sourceSearchAttribute                                              = chasm.NewSearchAttributeKeyword("HistoryDLQSourceCluster", chasm.SearchAttributeFieldKeyword01)
	targetSearchAttribute                                              = chasm.NewSearchAttributeKeyword("HistoryDLQTargetCluster", chasm.SearchAttributeFieldKeyword02)
	messageIDSearchAttribute                                           = chasm.NewSearchAttributeInt("HistoryDLQMessageId", chasm.SearchAttributeFieldInt02)
	enqueuedAtSearchAttribute                                          = chasm.NewSearchAttributeDateTime("HistoryDLQEnqueuedAt", chasm.SearchAttributeFieldDateTime01)
	pendingSearchAttribute                                             = chasm.NewSearchAttributeBool("HistoryDLQPending", chasm.SearchAttributeFieldBool01)
)

// NewMessage runs only in the execution creation callback. Enqueue retries must
// read the existing execution rather than construct a new message.
func NewMessage(ctx chasm.MutableContext, req EnqueueRequest) (*Message, error) {
	encoding := req.Task.GetBlob().GetEncodingType()
	if req.Task.GetShardId() != req.SourceTaskIdentity.SourceShardID || (encoding != enumspb.ENCODING_TYPE_PROTO3 && encoding != enumspb.ENCODING_TYPE_JSON) || len(req.Task.GetBlob().GetData()) == 0 {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a nonempty supported task with the original source shard")
	}
	c := &Message{}
	now := ctx.Now(c)
	id, err := messageID(req.Queue, req.SourceTaskIdentity, now)
	if err != nil {
		return nil, err
	}
	c.MessageState = &historydlqpb.MessageState{
		Queue: &commonspb.HistoryDLQKey{TaskCategory: req.Queue.CategoryID, SourceCluster: req.Queue.SourceCluster, TargetCluster: req.Queue.TargetCluster},
		Original: &historydlqpb.TaskIdentity{SourceShardId: req.SourceTaskIdentity.SourceShardID, TaskId: req.SourceTaskIdentity.TaskID, NamespaceId: req.SourceTaskIdentity.NamespaceID,
			BusinessId: req.SourceTaskIdentity.BusinessID, RunId: req.SourceTaskIdentity.RunID, ArchetypeId: req.SourceTaskIdentity.ArchetypeID},
		MessageId: id, EnqueuedAt: timestamppb.New(now), Status: historydlqpb.MESSAGE_STATUS_PENDING,
	}
	c.Task = chasm.NewDataField(ctx, proto.Clone(req.Task).(*commonspb.HistoryTask))
	c.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibility(ctx))
	return c, nil
}

func (c *Message) Read(ctx chasm.Context) (StoredMessage, error) {
	task, ok := c.Task.TryGet(ctx)
	if !ok {
		return StoredMessage{}, serviceerror.NewDataLoss("history DLQ message is missing its task")
	}
	key := ctx.ExecutionKey()
	queue := c.GetQueue()
	return StoredMessage{
		Ref: MessageRef{BusinessID: key.BusinessID, RunID: key.RunID, MessageID: c.GetMessageId(),
			Queue: QueueKey{CategoryID: queue.GetTaskCategory(), SourceCluster: queue.GetSourceCluster(), TargetCluster: queue.GetTargetCluster()}, EnqueuedAt: c.GetEnqueuedAt().AsTime()},
		SourceTaskIdentity: taskIdentity(c.GetOriginal()), Status: c.GetStatus(), Resolution: c.GetResolution(), Task: proto.Clone(task).(*commonspb.HistoryTask),
	}, nil
}

func (c *Message) Resolve(ctx chasm.MutableContext, reason historydlqpb.Resolution, enqueueTimeCutoff time.Time) error {
	if reason != historydlqpb.RESOLUTION_MERGED && reason != historydlqpb.RESOLUTION_PURGED {
		return serviceerror.NewInvalidArgument("invalid history DLQ resolution")
	}
	if c.GetStatus() == historydlqpb.MESSAGE_STATUS_RESOLVED {
		return nil
	}
	if c.GetStatus() != historydlqpb.MESSAGE_STATUS_PENDING || c.GetEnqueuedAt() == nil || c.GetEnqueuedAt().CheckValid() != nil {
		return serviceerror.NewFailedPrecondition("history DLQ message has invalid pending state")
	}
	if !Eligible(c.GetEnqueuedAt().AsTime(), enqueueTimeCutoff) {
		return serviceerror.NewFailedPrecondition("history DLQ message is newer than the captured enqueue time cutoff or the cutoff is missing")
	}
	c.Status = historydlqpb.MESSAGE_STATUS_RESOLVED
	c.Resolution = reason
	c.ResolvedAt = timestamppb.New(ctx.Now(c))
	return nil
}

func (c *Message) LifecycleState(chasm.Context) chasm.LifecycleState {
	if c.GetStatus() == historydlqpb.MESSAGE_STATUS_RESOLVED {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (c *Message) Terminate(ctx chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	if !c.LifecycleState(ctx).IsClosed() {
		return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("pending history DLQ messages require explicit resolution")
	}
	// CONSIDER(chanric) see if we want a metric
	return chasm.TerminateComponentResponse{}, nil
}

func (c *Message) ContextMetadata(chasm.Context) map[string]string { return nil }

func (c *Message) SearchAttributes(chasm.Context) []chasm.SearchAttributeKeyValue {
	queue := c.GetQueue()
	return []chasm.SearchAttributeKeyValue{
		categorySearchAttribute.Value(int64(queue.GetTaskCategory())), sourceSearchAttribute.Value(queue.GetSourceCluster()), targetSearchAttribute.Value(queue.GetTargetCluster()),
		messageIDSearchAttribute.Value(c.GetMessageId()), enqueuedAtSearchAttribute.Value(c.GetEnqueuedAt().AsTime()), pendingSearchAttribute.Value(c.GetStatus() == historydlqpb.MESSAGE_STATUS_PENDING),
	}
}

func (c *Message) Memo(chasm.Context) proto.Message { return &emptypb.Empty{} }
