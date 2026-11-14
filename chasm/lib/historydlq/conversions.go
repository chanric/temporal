package historydlq

import (
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func messageRefToProto(messageRef MessageRef) *historydlqpb.MessageRef {
	return &historydlqpb.MessageRef{
		BusinessId: messageRef.BusinessID,
		RunId:      messageRef.RunID,
		EntryKey:   messageRef.EntryKey,
		MessageId:  messageRef.MessageID,
		Queue:      queueKeyToProto(messageRef.Queue),
		EnqueuedAt: timestamppb.New(messageRef.EnqueuedAt),
	}
}

func messageProtoToRef(messageRefProto *historydlqpb.MessageRef) (MessageRef, error) {
	if messageRefProto.GetEnqueuedAt() == nil || messageRefProto.GetEnqueuedAt().CheckValid() != nil {
		return MessageRef{}, serviceerror.NewInvalidArgument("history DLQ requires a valid enqueue timestamp")
	}
	result := MessageRef{
		BusinessID: messageRefProto.GetBusinessId(),
		RunID:      messageRefProto.GetRunId(),
		EntryKey:   messageRefProto.GetEntryKey(),
		MessageID:  messageRefProto.GetMessageId(),
		Queue:      queueProtoToKey(messageRefProto.GetQueue()),
		EnqueuedAt: messageRefProto.GetEnqueuedAt().AsTime(),
	}
	return result, validateMessageRef(result)
}

func messageProtoToEntryRef(messageRefProto *historydlqpb.MessageRef) (MessageRef, error) {
	result := MessageRef{
		BusinessID: messageRefProto.GetBusinessId(),
		RunID:      messageRefProto.GetRunId(),
		EntryKey:   messageRefProto.GetEntryKey(),
		Queue:      queueProtoToKey(messageRefProto.GetQueue()),
	}
	return result, validateEntryRef(result)
}

func queueKeyToProto(queue QueueKey) *commonspb.HistoryDLQKey {
	return &commonspb.HistoryDLQKey{
		TaskCategory:  queue.CategoryID,
		SourceCluster: queue.SourceCluster,
		TargetCluster: queue.TargetCluster,
	}
}

func queueProtoToKey(queue *commonspb.HistoryDLQKey) QueueKey {
	return QueueKey{
		CategoryID:    queue.GetTaskCategory(),
		SourceCluster: queue.GetSourceCluster(),
		TargetCluster: queue.GetTargetCluster(),
	}
}

func taskIdentityToProto(identity TaskIdentity) *historydlqpb.TaskIdentity {
	return &historydlqpb.TaskIdentity{
		SourceShardId: identity.SourceShardID,
		TaskId:        identity.TaskID,
		NamespaceId:   identity.NamespaceID,
		BusinessId:    identity.BusinessID,
		RunId:         identity.RunID,
		ArchetypeId:   identity.ArchetypeID,
	}
}

func taskIdentityProtoToIdentity(identity *historydlqpb.TaskIdentity) TaskIdentity {
	return TaskIdentity{
		SourceShardID: identity.GetSourceShardId(),
		TaskID:        identity.GetTaskId(),
		NamespaceID:   identity.GetNamespaceId(),
		BusinessID:    identity.GetBusinessId(),
		RunID:         identity.GetRunId(),
		ArchetypeID:   identity.GetArchetypeId(),
	}
}

func taskIdentityToEntryProto(identity TaskIdentity) *historydlqpb.EntryTaskIdentity {
	return &historydlqpb.EntryTaskIdentity{
		TaskId:      identity.TaskID,
		NamespaceId: identity.NamespaceID,
		BusinessId:  identity.BusinessID,
		RunId:       identity.RunID,
		ArchetypeId: identity.ArchetypeID,
	}
}

func entryTaskIdentityProtoToIdentity(identity *historydlqpb.EntryTaskIdentity, sourceShardID int32) TaskIdentity {
	return TaskIdentity{
		SourceShardID: sourceShardID,
		TaskID:        identity.GetTaskId(),
		NamespaceID:   identity.GetNamespaceId(),
		BusinessID:    identity.GetBusinessId(),
		RunID:         identity.GetRunId(),
		ArchetypeID:   identity.GetArchetypeId(),
	}
}

func segmentRefToProto(ref SegmentRef) *historydlqpb.SegmentRef {
	return &historydlqpb.SegmentRef{Queue: queueKeyToProto(ref.Queue), BusinessId: ref.BusinessID, RunId: ref.RunID}
}

func segmentProtoToRef(ref *historydlqpb.SegmentRef) (SegmentRef, error) {
	result := SegmentRef{Queue: queueProtoToKey(ref.GetQueue()), BusinessID: ref.GetBusinessId(), RunID: ref.GetRunId()}
	return result, validateSegmentRef(result)
}

func storedMessageToProto(message StoredMessage) *historydlqpb.StoredMessage {
	return &historydlqpb.StoredMessage{Ref: messageRefToProto(message.Ref), SourceTaskIdentity: taskIdentityToProto(message.SourceTaskIdentity), Task: message.Task}
}
