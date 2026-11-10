package dlq

import (
	"context"
	"errors"
	"math"
	"slices"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"google.golang.org/grpc/codes"
)

const mergeEntriesActivityName = "dlq-merge-segment-entries-activity"

type MergeEntriesParams struct {
	Segment historydlq.SegmentRef
	Refs    []historydlq.MessageRef
}

type MergeEntryFailure struct {
	Ref          historydlq.MessageRef
	Error        string
	NonRetryable bool
}

type MergeEntriesResult struct {
	Failures []MergeEntryFailure
	// ResolvedCount includes selected keys already absent in the exact segment run.
	ResolvedCount int64
}

type mergeSubmission struct {
	ref  historydlq.MessageRef
	task *commonspb.HistoryTask
}

// mergeEntries returns only failed references and a confirmed-resolution count.
func (c *workerComponent) mergeEntries(ctx context.Context, params MergeEntriesParams) (MergeEntriesResult, error) {
	refs, err := c.mergeEntryRefs(params)
	if err != nil {
		return MergeEntriesResult{}, c.convertServerErr(err, "invalid DLQ merge request")
	}
	readFailures, submissions, err := c.readMergeEntries(ctx, params.Segment, refs)
	if err != nil {
		return MergeEntriesResult{}, err
	}
	result := MergeEntriesResult{Failures: readFailures}
	// Re-enqueue payloads into their original history task queues.
	var reEnqueuedRefs []historydlq.MessageRef
	for batch := range slices.Chunk(submissions, MaxMergeBatchSize) {
		if err := ctx.Err(); err != nil {
			return MergeEntriesResult{}, err
		}
		failures, err := c.reEnqueueMergeBatch(ctx, params.Segment.Queue, batch)
		for index, submission := range batch {
			if err != nil {
				result.Failures = append(result.Failures, c.mergeEntryFailure(submission.ref, err))
			} else if failure, failed := failures[index]; failed {
				result.Failures = append(result.Failures, MergeEntryFailure{
					Ref: submission.ref, Error: failure.Message, NonRetryable: failure.NonRetryable,
				})
			} else {
				reEnqueuedRefs = append(reEnqueuedRefs, submission.ref)
			}
		}
	}
	// Delete DLQ entries only after their tasks were confirmed re-enqueued.
	for batch := range slices.Chunk(reEnqueuedRefs, historydlq.MaxResolveBatchSize) {
		if err := ctx.Err(); err != nil {
			return MergeEntriesResult{}, err
		}
		var entryKeys []string
		for _, ref := range batch {
			entryKeys = append(entryKeys, ref.EntryKey)
		}
		err := c.chasmClient.ResolveEntries(ctx, historydlq.ResolveEntriesRequest{
			Segment: params.Segment, EntryKeys: entryKeys, Resolution: historydlqpb.RESOLUTION_MERGED,
		})
		if err != nil {
			for _, ref := range batch {
				result.Failures = append(result.Failures, c.mergeEntryFailure(ref, err))
			}
		}
	}
	result.ResolvedCount = int64(len(refs) - len(result.Failures))
	return result, nil
}

func (c *workerComponent) readMergeEntries(
	ctx context.Context, segment historydlq.SegmentRef, refs []historydlq.MessageRef,
) ([]MergeEntryFailure, []mergeSubmission, error) {
	var failures []MergeEntryFailure
	var submissions []mergeSubmission
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		stored, alreadyResolved, err := c.readMergeEntry(ctx, segment, ref)
		if err != nil {
			failures = append(failures, c.mergeEntryFailure(ref, err))
		} else if !alreadyResolved {
			submissions = append(submissions, mergeSubmission{ref: stored.Ref, task: stored.Task})
		}
	}
	return failures, submissions, nil
}

func (c *workerComponent) reEnqueueMergeBatch(
	ctx context.Context, queue historydlq.QueueKey, batch []mergeSubmission,
) (map[int]*commonspb.AddTaskFailure, error) {
	client, err := c.taskClientDialer.Dial(ctx, queue.SourceCluster)
	if err != nil {
		return nil, err
	}
	request := &adminservice.AddTasksRequest{ShardId: batch[0].task.ShardId, ReturnTaskFailures: true}
	for _, submission := range batch {
		request.Tasks = append(request.Tasks, &adminservice.AddTasksRequest_Task{
			CategoryId: queue.CategoryID, Blob: submission.task.Blob,
		})
	}
	response, err := client.AddTasks(ctx, request)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, serviceerror.NewDataLoss("AddTasks returned no response")
	}
	failures := make(map[int]*commonspb.AddTaskFailure, len(response.Failures))
	for _, failure := range response.Failures {
		if failure == nil || failure.TaskIndex < 0 || int(failure.TaskIndex) >= len(batch) {
			return nil, serviceerror.NewDataLoss("AddTasks returned an invalid failed task index")
		}
		failures[int(failure.TaskIndex)] = failure
	}
	return failures, nil
}

func (c *workerComponent) mergeEntryRefs(params MergeEntriesParams) ([]historydlq.MessageRef, error) {
	_, err := c.segmentJobQueueKey(SegmentJobParams{Key: Key{
		TaskCategoryID: int(params.Segment.Queue.CategoryID),
		SourceCluster:  params.Segment.Queue.SourceCluster,
		TargetCluster:  params.Segment.Queue.TargetCluster,
	}})
	if err != nil {
		return nil, err
	}
	var refs []historydlq.MessageRef
	seen := make(map[string]bool, len(params.Refs))
	for _, ref := range params.Refs {
		if ref.Queue != params.Segment.Queue || ref.BusinessID != params.Segment.BusinessID ||
			ref.RunID != params.Segment.RunID || ref.EntryKey == "" {
			return nil, serviceerror.NewInvalidArgument("DLQ merge requires entry references from one exact segment run")
		}
		if !seen[ref.EntryKey] {
			seen[ref.EntryKey] = true
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

func (c *workerComponent) mergeEntryFailure(ref historydlq.MessageRef, err error) MergeEntryFailure {
	failure := MergeEntryFailure{Ref: ref, Error: err.Error()}
	converted := c.convertServerErr(err, "DLQ merge failed")
	if applicationError, ok := errors.AsType[*temporal.ApplicationError](converted); ok {
		failure.NonRetryable = applicationError.NonRetryable()
	}
	return failure
}

// readMergeEntry refreshes a stale reference once before reading its payload again.
// alreadyResolved means its key was absent from metadata listing in the exact run.
func (c *workerComponent) readMergeEntry(
	ctx context.Context, segment historydlq.SegmentRef, ref historydlq.MessageRef,
) (message historydlq.StoredMessage, alreadyResolved bool, err error) {
	message, err = c.chasmClient.ReadMessage(ctx, ref)
	if err == nil {
		return message, false, nil
	}
	code := serviceerror.ToStatus(err).Code()
	if code != codes.NotFound && code != codes.FailedPrecondition {
		return historydlq.StoredMessage{}, false, err
	}
	// A recreated selected key can have a newer ID than the original request bound.
	request := historydlq.ListEntriesRequest{
		Segment: segment, InclusiveMaxMessageID: math.MaxInt64, PageSize: segmentPageSize,
	}
	for {
		if err := ctx.Err(); err != nil {
			return historydlq.StoredMessage{}, false, err
		}
		page, err := c.chasmClient.ListEntries(ctx, request)
		if err != nil {
			return historydlq.StoredMessage{}, false, err
		}
		for _, currentRef := range page.Refs {
			if currentRef.EntryKey == ref.EntryKey {
				message, err := c.chasmClient.ReadMessage(ctx, currentRef)
				return message, false, err
			}
		}
		if len(page.NextPageToken) == 0 {
			return historydlq.StoredMessage{}, true, nil
		}
		request.NextPageToken = page.NextPageToken
	}
}
