package historydlq

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/softassert"
)

type Client struct {
	rpc    historydlqpb.HistoryDLQServiceClient
	logger log.Logger
}

func NewClient(logger log.Logger, rpc historydlqpb.HistoryDLQServiceClient) *Client {
	return &Client{rpc: rpc, logger: logger}
}

func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (MessageRef, error) {
	segmentBusinessID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	if err != nil {
		return MessageRef{}, err
	}
	response, err := c.rpc.Enqueue(ctx, &historydlqpb.EnqueueRequest{
		SystemNamespaceId:  primitives.SystemNamespaceID,
		BusinessId:         segmentBusinessID,
		Queue:              queueKeyToProto(req.Queue),
		SourceTaskIdentity: taskIdentityToProto(req.SourceTaskIdentity),
		Task:               req.Task,
	})
	if err != nil {
		return MessageRef{}, err
	}
	messageRef, err := messageProtoToRef(response.GetRef())
	entryKey, _ := EntryKey(req.Queue, req.SourceTaskIdentity)
	if err != nil ||
		messageRef.BusinessID != segmentBusinessID ||
		messageRef.Queue != req.Queue ||
		messageRef.EntryKey != entryKey {
		return MessageRef{}, serviceerror.NewDataLoss("history DLQ enqueue returned an invalid reference")
	}
	return messageRef, nil
}

func (c *Client) ReadMessage(ctx context.Context, messageRef MessageRef) (StoredMessage, error) {
	if err := validateMessageRef(messageRef); err != nil {
		return StoredMessage{}, err
	}
	response, err := c.rpc.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               messageRefToProto(messageRef),
	})
	if err != nil {
		return StoredMessage{}, err
	}
	message := response.GetMessage()
	returnedRef, err := messageProtoToRef(message.GetRef())
	if err != nil || !sameEntryReference(returnedRef, messageRef) {
		return StoredMessage{}, softassert.UnexpectedInternalErr(
			c.logger,
			"history DLQ read returned an invalid reference",
			err,
		)
	}
	sourceTaskIdentity := taskIdentityProtoToIdentity(message.GetSourceTaskIdentity())
	segmentBusinessID, err := SegmentBusinessID(messageRef.Queue, sourceTaskIdentity)
	entryKey, _ := EntryKey(messageRef.Queue, sourceTaskIdentity)
	if err != nil || segmentBusinessID != messageRef.BusinessID || entryKey != messageRef.EntryKey {
		return StoredMessage{}, softassert.UnexpectedInternalErr(
			c.logger,
			"history DLQ read returned invalid task identity",
			nil,
		)
	}
	task := message.GetTask()
	encoding := task.GetBlob().GetEncodingType()
	if task.GetShardId() != sourceTaskIdentity.SourceShardID ||
		len(task.GetBlob().GetData()) == 0 ||
		(encoding != enumspb.ENCODING_TYPE_PROTO3 && encoding != enumspb.ENCODING_TYPE_JSON) {
		return StoredMessage{}, softassert.UnexpectedInternalErr(
			c.logger,
			"history DLQ read returned invalid task payload",
			nil,
		)
	}
	return StoredMessage{
		Ref:                returnedRef,
		SourceTaskIdentity: sourceTaskIdentity,
		Task:               common.CloneProto(message.GetTask()),
	}, nil
}

func (c *Client) Resolve(
	ctx context.Context,
	messageRef MessageRef,
	resolutionReason historydlqpb.Resolution,
) error {
	if err := validateEntryRef(messageRef); err != nil {
		return err
	}
	if err := validateResolutionReason(resolutionReason); err != nil {
		return err
	}
	_, err := c.rpc.Resolve(ctx, &historydlqpb.ResolveRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Ref:               messageRefToProto(messageRef),
		Resolution:        resolutionReason,
	})
	return err
}
