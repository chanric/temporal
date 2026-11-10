package dlq

import (
	"context"
	"slices"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
)

const purgeEntriesActivityName = "dlq-purge-segment-entries-activity"

type PurgeEntriesParams struct {
	Segment   historydlq.SegmentRef
	EntryKeys []string
}

type PurgeEntriesResult struct {
	// ResolvedEntryKeys includes requested keys already absent in the exact segment run.
	ResolvedEntryKeys []string
}

func (c *workerComponent) purgeEntries(ctx context.Context, params PurgeEntriesParams) (PurgeEntriesResult, error) {
	_, err := c.segmentJobQueueKey(SegmentJobParams{Key: Key{
		TaskCategoryID: int(params.Segment.Queue.CategoryID),
		SourceCluster:  params.Segment.Queue.SourceCluster,
		TargetCluster:  params.Segment.Queue.TargetCluster,
	}})
	if err != nil {
		return PurgeEntriesResult{}, c.convertServerErr(err, "invalid DLQ purge request")
	}
	var entryKeys []string
	seen := make(map[string]struct{}, len(params.EntryKeys))
	for _, entryKey := range params.EntryKeys {
		if entryKey == "" {
			return PurgeEntriesResult{}, c.convertServerErr(
				serviceerror.NewInvalidArgument("DLQ purge requires nonempty entry keys"), "invalid DLQ purge request",
			)
		}
		if _, exists := seen[entryKey]; !exists {
			seen[entryKey] = struct{}{}
			entryKeys = append(entryKeys, entryKey)
		}
	}
	for batch := range slices.Chunk(entryKeys, historydlq.MaxResolveBatchSize) {
		if err := ctx.Err(); err != nil {
			return PurgeEntriesResult{}, err
		}
		err := c.chasmClient.ResolveEntries(ctx, historydlq.ResolveEntriesRequest{
			Segment:    params.Segment,
			EntryKeys:  batch,
			Resolution: historydlqpb.RESOLUTION_PURGED,
		})
		if err != nil {
			return PurgeEntriesResult{}, c.convertServerErr(err, "DLQ purge failed")
		}
	}
	return PurgeEntriesResult{ResolvedEntryKeys: entryKeys}, nil
}
