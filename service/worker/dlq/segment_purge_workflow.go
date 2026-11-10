package dlq

import (
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/primitives"
)

func (c *workerComponent) purgeSegment(ctx workflow.Context, req historydlq.ListEntriesRequest) (int64, error) {
	readContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           primitives.DLQActivityTQ,
		StartToCloseTimeout: mergeTasksActivityTimeout,
		RetryPolicy:         mergeActivityRetryPolicy,
	})
	purgeContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           primitives.DLQActivityTQ,
		StartToCloseTimeout: deleteTasksActivityTimeout,
		RetryPolicy:         deleteActivityRetryPolicy,
	})
	var processed int64
	for {
		var page historydlq.EntryPage
		if err := workflow.ExecuteActivity(readContext, listSegmentEntriesActivityName, req).Get(ctx, &page); err != nil {
			return processed, err
		}
		if len(page.Refs) > 0 {
			params := PurgeEntriesParams{Segment: req.Segment}
			for _, ref := range page.Refs {
				params.EntryKeys = append(params.EntryKeys, ref.EntryKey)
			}
			var result PurgeEntriesResult
			if err := workflow.ExecuteActivity(purgeContext, purgeEntriesActivityName, params).Get(ctx, &result); err != nil {
				return processed, err
			}
			processed += int64(len(result.ResolvedEntryKeys))
		}
		if len(page.NextPageToken) == 0 {
			return processed, nil
		}
		req.NextPageToken = page.NextPageToken
	}
}
