package dlq

import (
	"context"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/historydlq"
)

const (
	segmentPageSize                = 256
	discoverSegmentsActivityName   = "dlq-discover-segments-activity"
	listSegmentEntriesActivityName = "dlq-list-segment-entries-activity"
)

type SegmentJobParams struct {
	Key          Key
	MaxMessageID int64
}

type segmentMetadataClient interface {
	ListEntries(context.Context, historydlq.ListEntriesRequest) (historydlq.EntryPage, error)
}

func (c *workerComponent) segmentJobQueueKey(params SegmentJobParams) (historydlq.QueueKey, error) {
	key := params.Key
	if key.TaskCategoryID <= 0 || key.TaskCategoryID > math.MaxInt32 ||
		key.SourceCluster == "" || key.TargetCluster == "" || key.TargetCluster != c.currentClusterName ||
		params.MaxMessageID < 0 {
		return historydlq.QueueKey{}, serviceerror.NewInvalidArgument(
			"DLQ job requires a local queue and nonnegative message bound",
		)
	}
	return historydlq.QueueKey{
		CategoryID:    int32(key.TaskCategoryID),
		SourceCluster: key.SourceCluster,
		TargetCluster: key.TargetCluster,
	}, nil
}

func (c *workerComponent) discoverSegments(ctx context.Context, params SegmentJobParams) ([]historydlq.SegmentRef, error) {
	queue, err := c.segmentJobQueueKey(params)
	if err != nil {
		return nil, c.convertServerErr(err, "invalid DLQ job request")
	}
	segments, err := historydlq.DiscoverSegments(ctx, c.visibilityManager, queue, params.MaxMessageID)
	if err != nil {
		return nil, c.convertServerErr(err, "DLQ segment discovery failed")
	}
	return segments, nil
}

func (c *workerComponent) listSegmentEntries(
	ctx context.Context,
	req historydlq.ListEntriesRequest,
) (historydlq.EntryPage, error) {
	_, err := c.segmentJobQueueKey(SegmentJobParams{
		Key: Key{
			TaskCategoryID: int(req.Segment.Queue.CategoryID),
			SourceCluster:  req.Segment.Queue.SourceCluster,
			TargetCluster:  req.Segment.Queue.TargetCluster,
		},
		MaxMessageID: req.InclusiveMaxMessageID,
	})
	if err != nil {
		return historydlq.EntryPage{}, c.convertServerErr(err, "invalid DLQ job request")
	}
	if req.PageSize == 0 {
		req.PageSize = segmentPageSize
	}
	page, err := c.chasmClient.ListEntries(ctx, req)
	if err != nil {
		return historydlq.EntryPage{}, c.convertServerErr(err, "DLQ entry listing failed")
	}
	return page, nil
}
