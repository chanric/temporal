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
	return c.storedMessageProtoToMessage(response.GetMessage(), messageRef)
}

func (c *Client) storedMessageProtoToMessage(
	message *historydlqpb.StoredMessage,
	messageRef MessageRef,
) (StoredMessage, error) {
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

func (c *Client) ListEntries(ctx context.Context, req ListEntriesRequest) (EntryPage, error) {
	if err := validateListEntriesRequest(req); err != nil {
		return EntryPage{}, err
	}
	if int64(req.PageSize) > 1<<31-1 {
		return EntryPage{}, serviceerror.NewInvalidArgument("history DLQ page size exceeds int32")
	}
	response, err := c.rpc.ListEntries(ctx, &historydlqpb.ListEntriesRequest{
		SystemNamespaceId:     primitives.SystemNamespaceID,
		Segment:               segmentRefToProto(req.Segment),
		InclusiveMaxMessageId: req.InclusiveMaxMessageID,
		PageSize:              int32(req.PageSize),
		NextPageToken:         req.NextPageToken,
	})
	if err != nil {
		return EntryPage{}, err
	}
	return entryProtoPageToPage(req, response)
}

func entryProtoPageToPage(
	req ListEntriesRequest,
	response *historydlqpb.ListEntriesResponse,
) (EntryPage, error) {
	invalid := func() (EntryPage, error) {
		return EntryPage{}, serviceerror.NewDataLoss("history DLQ list returned invalid metadata")
	}
	if response == nil || len(response.GetRefs()) > req.PageSize {
		return invalid()
	}
	page := EntryPage{NextPageToken: append([]byte(nil), response.GetNextPageToken()...)}
	for _, messageRefProto := range response.GetRefs() {
		ref, err := messageProtoToRef(messageRefProto)
		if err != nil ||
			ref.Queue != req.Segment.Queue ||
			ref.BusinessID != req.Segment.BusinessID ||
			ref.RunID != req.Segment.RunID ||
			ref.MessageID > req.InclusiveMaxMessageID {
			return invalid()
		}
		page.Refs = append(page.Refs, ref)
	}
	return page, nil
}

func (c *Client) DescribeSegment(ctx context.Context, ref SegmentRef) (SegmentStats, error) {
	if err := validateSegmentRef(ref); err != nil {
		return SegmentStats{}, err
	}
	response, err := c.rpc.DescribeSegment(ctx, &historydlqpb.DescribeSegmentRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Segment:           segmentRefToProto(ref),
	})
	if err != nil {
		return SegmentStats{}, err
	}
	if response == nil ||
		response.GetPendingCount() < 0 ||
		response.GetLastMessageId() < 0 ||
		(response.GetPendingCount() == 0 && response.GetLastMessageId() != 0) {
		return SegmentStats{}, serviceerror.NewDataLoss("history DLQ describe returned invalid statistics")
	}
	return SegmentStats{
		PendingCount:  response.GetPendingCount(),
		LastMessageID: response.GetLastMessageId(),
	}, nil
}

func (c *Client) ReadSegmentMessages(ctx context.Context, req ReadSegmentMessagesRequest) (MessagePage, error) {
	metadataReq := ListEntriesRequest(req)
	if err := validateListEntriesRequest(metadataReq); err != nil {
		return MessagePage{}, err
	}
	if int64(req.PageSize) > 1<<31-1 {
		return MessagePage{}, serviceerror.NewInvalidArgument("history DLQ page size exceeds int32")
	}
	response, err := c.rpc.ReadSegmentMessages(ctx, &historydlqpb.ReadSegmentMessagesRequest{
		SystemNamespaceId:     primitives.SystemNamespaceID,
		Segment:               segmentRefToProto(req.Segment),
		InclusiveMaxMessageId: req.InclusiveMaxMessageID,
		PageSize:              int32(req.PageSize),
		NextPageToken:         req.NextPageToken,
	})
	if err != nil {
		return MessagePage{}, err
	}
	if response == nil {
		return MessagePage{}, serviceerror.NewDataLoss("history DLQ segment read returned no response")
	}
	metadataResponse := &historydlqpb.ListEntriesResponse{NextPageToken: response.NextPageToken}
	for _, message := range response.Messages {
		metadataResponse.Refs = append(metadataResponse.Refs, message.GetRef())
	}
	entries, err := entryProtoPageToPage(metadataReq, metadataResponse)
	if err != nil {
		return MessagePage{}, err
	}
	page := MessagePage{NextPageToken: entries.NextPageToken}
	for i, storedMessageProto := range response.Messages {
		message, err := c.storedMessageProtoToMessage(storedMessageProto, entries.Refs[i])
		if err != nil {
			return MessagePage{}, err
		}
		page.Messages = append(page.Messages, message)
	}
	return page, nil
}

func (c *Client) ResolveEntries(ctx context.Context, req ResolveEntriesRequest) error {
	if err := validateResolveEntriesRequest(req); err != nil {
		return err
	}
	_, err := c.rpc.ResolveEntries(ctx, &historydlqpb.ResolveEntriesRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		Segment:           segmentRefToProto(req.Segment),
		EntryKeys:         req.EntryKeys,
		Resolution:        req.Resolution,
	})
	return err
}
