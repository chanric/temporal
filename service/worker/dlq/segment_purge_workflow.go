package dlq

import (
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/primitives"
)

func (c *workerComponent) purgeSegment(
	ctx workflow.Context, req historydlq.ListEntriesRequest, progress *ProgressQueryResponse,
) error {
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
	for {
		var page historydlq.EntryPage
		if err := workflow.ExecuteActivity(readContext, listSegmentEntriesActivityName, req).Get(ctx, &page); err != nil {
			return err
		}
		if len(page.Refs) > 0 {
			params := PurgeEntriesParams{Segment: req.Segment}
			for _, ref := range page.Refs {
				params.EntryKeys = append(params.EntryKeys, ref.EntryKey)
			}
			var result PurgeEntriesResult
			if err := workflow.ExecuteActivity(purgeContext, purgeEntriesActivityName, params).Get(ctx, &result); err != nil {
				return err
			}
			recordSegmentProgress(progress, page.Refs, int64(len(result.ResolvedEntryKeys)), nil)
		}
		if len(page.NextPageToken) == 0 {
			return nil
		}
		req.NextPageToken = page.NextPageToken
	}
}
