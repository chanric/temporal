package historydlq

import (
	"context"
	"errors"
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/softassert"
)

type serviceHandler struct {
	historydlqpb.UnimplementedHistoryDLQServiceServer
	logger         log.Logger
	metricsHandler metrics.Handler
	currentCluster string
}

func newServiceHandler(logger log.Logger, metadata cluster.Metadata, metricsHandler metrics.Handler) *serviceHandler {
	return &serviceHandler{logger: logger, metricsHandler: metricsHandler, currentCluster: metadata.GetCurrentClusterName()}
}

// Make sure we routed to the correct server
func (h *serviceHandler) validateLocalRouting(namespaceID string, queue QueueKey) error {
	if namespaceID != primitives.SystemNamespaceID || queue.TargetCluster != h.currentCluster {
		return serviceerror.NewInvalidArgument("history DLQ routing must match the local system execution identity")
	}
	return nil
}

func (h *serviceHandler) Enqueue(
	ctx context.Context,
	req *historydlqpb.EnqueueRequest,
) (*historydlqpb.EnqueueResponse, error) {
	enqueueRequest := EnqueueRequest{
		Queue:              queueProtoToKey(req.GetQueue()),
		SourceTaskIdentity: taskIdentityProtoToIdentity(req.GetSourceTaskIdentity()),
		Task:               req.GetTask(),
	}
	segmentBusinessID, err := SegmentBusinessID(enqueueRequest.Queue, enqueueRequest.SourceTaskIdentity)
	if err != nil {
		return nil, err
	}
	if err := h.validateLocalRouting(req.GetSystemNamespaceId(), enqueueRequest.Queue); err != nil {
		return nil, err
	}
	if req.GetBusinessId() != segmentBusinessID {
		return nil, serviceerror.NewInvalidArgument("history DLQ enqueue routing does not match the segment identity")
	}
	result, err := chasm.UpdateWithStartExecution[*Segment](
		ctx,
		chasm.ExecutionKey{
			NamespaceID: primitives.SystemNamespaceID,
			BusinessID:  segmentBusinessID,
		},
		NewSegment,
		func(s *Segment, ctx chasm.MutableContext, enqueueRequest EnqueueRequest) (appendResult, error) {
			return s.Append(ctx, enqueueRequest)
		},
		enqueueRequest,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	metricTags := []metrics.Tag{
		metrics.TaskCategoryTag(strconv.Itoa(int(enqueueRequest.Queue.CategoryID))),
		metrics.SourceClusterTag(enqueueRequest.Queue.SourceCluster),
		metrics.TargetClusterTag(enqueueRequest.Queue.TargetCluster),
	}
	if err != nil {
		if errors.Is(err, segmentBudgetExceededErr) {
			metrics.HistoryDLQSegmentBudgetExceeded.With(h.metricsHandler).Record(1, metricTags...)
		}
		return nil, err
	}
	messageRef := result.UpdateOutput.MessageRef
	entryKey, _ := EntryKey(enqueueRequest.Queue, enqueueRequest.SourceTaskIdentity)
	// The accepted reference must address the requested task in the persisted segment run.
	if validateMessageRef(messageRef) != nil ||
		messageRef.BusinessID != segmentBusinessID ||
		messageRef.RunID != result.ExecutionKey.RunID ||
		messageRef.EntryKey != entryKey ||
		messageRef.Queue != enqueueRequest.Queue {
		return nil, softassert.UnexpectedInternalErr(
			h.logger,
			"history DLQ enqueue returned an invalid accepted entry",
			nil,
		)
	}
	if result.UpdateOutput.CrossedWarningThreshold {
		metrics.HistoryDLQSegmentWarningThresholdCrossed.With(h.metricsHandler).Record(1, metricTags...)
	}
	return &historydlqpb.EnqueueResponse{Ref: messageRefToProto(messageRef)}, nil
}

func (h *serviceHandler) ReadMessage(
	ctx context.Context,
	req *historydlqpb.ReadMessageRequest,
) (*historydlqpb.ReadMessageResponse, error) {
	messageRef, err := messageProtoToRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	if err := h.validateLocalRouting(req.GetSystemNamespaceId(), messageRef.Queue); err != nil {
		return nil, err
	}
	storedMessage, err := chasm.ReadComponent[*Segment](
		ctx,
		messageRefToSegmentComponentRef(messageRef),
		func(s *Segment, ctx chasm.Context, _ struct{}) (StoredMessage, error) {
			return s.ReadEntry(ctx, messageRef)
		},
		struct{}{},
	)
	if err != nil {
		return nil, err
	}
	return &historydlqpb.ReadMessageResponse{
		Message: &historydlqpb.StoredMessage{
			Ref:                messageRefToProto(storedMessage.Ref),
			SourceTaskIdentity: taskIdentityToProto(storedMessage.SourceTaskIdentity),
			Task:               storedMessage.Task,
		},
	}, nil
}

func (h *serviceHandler) Resolve(
	ctx context.Context,
	req *historydlqpb.ResolveRequest,
) (*historydlqpb.ResolveResponse, error) {
	messageRef, err := messageProtoToEntryRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	if err := h.validateLocalRouting(req.GetSystemNamespaceId(), messageRef.Queue); err != nil {
		return nil, err
	}
	if err := validateResolutionReason(req.GetResolution()); err != nil {
		return nil, err
	}
	_, _, err = chasm.UpdateComponent[*Segment](
		ctx,
		messageRefToSegmentComponentRef(messageRef),
		func(
			s *Segment,
			ctx chasm.MutableContext,
			_ struct{},
		) (struct{}, error) {
			return struct{}{}, s.ResolveEntry(ctx, messageRef, req.GetResolution())
		},
		struct{}{},
	)
	if err != nil {
		return nil, err
	}
	return &historydlqpb.ResolveResponse{}, nil
}

func messageRefToSegmentComponentRef(messageRef MessageRef) chasm.ComponentRef {
	return chasm.NewComponentRef[*Segment](chasm.ExecutionKey{
		NamespaceID: primitives.SystemNamespaceID,
		BusinessID:  messageRef.BusinessID,
		RunID:       messageRef.RunID,
	})
}

func (h *serviceHandler) ListEntries(ctx context.Context, req *historydlqpb.ListEntriesRequest) (*historydlqpb.ListEntriesResponse, error) {
	ref, err := segmentProtoToRef(req.GetSegment())
	if err != nil {
		return nil, err
	}
	if err := h.validateLocalRouting(req.GetSystemNamespaceId(), ref.Queue); err != nil {
		return nil, err
	}
	request := ListEntriesRequest{Segment: ref, InclusiveMaxMessageID: req.GetInclusiveMaxMessageId(), PageSize: int(req.GetPageSize()), NextPageToken: req.GetNextPageToken()}
	if err := validateListEntriesRequest(request); err != nil {
		return nil, err
	}
	page, err := chasm.ReadComponent[*Segment](ctx, segmentRefToComponentRef(ref), func(s *Segment, ctx chasm.Context, req ListEntriesRequest) (EntryPage, error) {
		return s.ListEntries(ctx, req)
	}, request)
	if err != nil {
		return nil, err
	}
	response := &historydlqpb.ListEntriesResponse{NextPageToken: page.NextPageToken}
	for _, messageRef := range page.Refs {
		response.Refs = append(response.Refs, messageRefToProto(messageRef))
	}
	return response, nil
}

func (h *serviceHandler) DescribeSegment(ctx context.Context, req *historydlqpb.DescribeSegmentRequest) (*historydlqpb.DescribeSegmentResponse, error) {
	ref, err := segmentProtoToRef(req.GetSegment())
	if err != nil {
		return nil, err
	}
	if err := h.validateLocalRouting(req.GetSystemNamespaceId(), ref.Queue); err != nil {
		return nil, err
	}
	stats, err := chasm.ReadComponent[*Segment](ctx, segmentRefToComponentRef(ref), func(s *Segment, ctx chasm.Context, ref SegmentRef) (SegmentStats, error) {
		return s.DescribeSegment(ctx, ref)
	}, ref)
	if err != nil {
		return nil, err
	}
	return &historydlqpb.DescribeSegmentResponse{PendingCount: stats.PendingCount, LastMessageId: stats.LastMessageID}, nil
}

func segmentRefToComponentRef(ref SegmentRef) chasm.ComponentRef {
	return chasm.NewComponentRef[*Segment](chasm.ExecutionKey{NamespaceID: primitives.SystemNamespaceID, BusinessID: ref.BusinessID, RunID: ref.RunID})
}

func (h *serviceHandler) ReadSegmentMessages(ctx context.Context, req *historydlqpb.ReadSegmentMessagesRequest) (*historydlqpb.ReadSegmentMessagesResponse, error) {
	ref, err := segmentProtoToRef(req.GetSegment())
	if err != nil {
		return nil, err
	}
	if err := h.validateLocalRouting(req.GetSystemNamespaceId(), ref.Queue); err != nil {
		return nil, err
	}
	request := ReadSegmentMessagesRequest{Segment: ref, InclusiveMaxMessageID: req.GetInclusiveMaxMessageId(), PageSize: int(req.GetPageSize()), NextPageToken: req.GetNextPageToken()}
	if err := validateListEntriesRequest(ListEntriesRequest(request)); err != nil {
		return nil, err
	}
	page, err := chasm.ReadComponent[*Segment](ctx, segmentRefToComponentRef(ref), func(s *Segment, ctx chasm.Context, req ReadSegmentMessagesRequest) (MessagePage, error) {
		return s.ReadSegmentMessages(ctx, req)
	}, request)
	if err != nil {
		return nil, err
	}
	response := &historydlqpb.ReadSegmentMessagesResponse{NextPageToken: page.NextPageToken}
	for _, message := range page.Messages {
		response.Messages = append(response.Messages, storedMessageToProto(message))
	}
	return response, nil
}
