package historydlq

import (
	"context"

	"go.temporal.io/api/serviceerror"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
	"golang.org/x/sync/errgroup"
)

func (s *Store) DiscoverSegments(ctx context.Context, queue dlq.QueueKey, inclusiveMaxMessageID int64) ([]dlq.SegmentRef, error) {
	return dlq.DiscoverSegments(ctx, s.visibilityManager, queue, inclusiveMaxMessageID)
}

func (s *Store) readCHASMMessages(ctx context.Context, queue dlq.QueueKey, inclusiveMaxMessageID int64) ([]dlq.StoredMessage, error) {
	segments, err := s.DiscoverSegments(ctx, queue, inclusiveMaxMessageID)
	if err != nil {
		return nil, err
	}
	messagesBySegment := make([][]dlq.StoredMessage, len(segments))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for i, segment := range segments {
		group.Go(func() error {
			if err := groupCtx.Err(); err != nil {
				return err
			}
			var err error
			messagesBySegment[i], err = s.readCHASMSegmentMessages(groupCtx, segment, inclusiveMaxMessageID)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	var messages []dlq.StoredMessage
	for _, segmentMessages := range messagesBySegment {
		messages = append(messages, segmentMessages...)
	}
	return messages, nil
}

func (s *Store) readCHASMSegmentMessages(ctx context.Context, segment dlq.SegmentRef, inclusiveMaxMessageID int64) ([]dlq.StoredMessage, error) {
	request := dlq.ReadSegmentMessagesRequest{Segment: segment, InclusiveMaxMessageID: inclusiveMaxMessageID, PageSize: 256}
	var messages []dlq.StoredMessage
	tokens := map[string]struct{}{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := s.chasmClient.ReadSegmentMessages(ctx, request)
		if err != nil {
			return nil, err
		}
		messages = append(messages, page.Messages...)
		if len(page.NextPageToken) == 0 {
			return messages, nil
		}
		token := string(page.NextPageToken)
		if _, ok := tokens[token]; ok {
			return nil, serviceerror.NewInternal("history DLQ segment read repeated a page token")
		}
		tokens[token] = struct{}{}
		request.NextPageToken = page.NextPageToken
	}
}
