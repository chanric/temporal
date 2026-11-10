package dlq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/primitives"
)

const (
	// MixedWorkflowName processes both stores; WorkflowName remains registered for existing QueueV2 histories.
	MixedWorkflowName                = "temporal-sys-dlq-mixed-workflow"
	deleteTasksIfPresentActivityName = "dlq-delete-tasks-if-present-activity"
	errorTypeUnresolvedEntries       = "dlq-error-type-unresolved-entries"
)

func (c *workerComponent) workflow(ctx workflow.Context, params WorkflowParams) error {
	progress := ProgressQueryResponse{WorkflowType: params.WorkflowType}
	if err := workflow.SetQueryHandler(ctx, QueryTypeProgress, func() (ProgressQueryResponse, error) { return progress, nil }); err != nil {
		return err
	}
	pageSize := segmentPageSize
	switch params.WorkflowType {
	case WorkflowTypeDelete:
		progress.DlqKey, progress.MaxMessageIDToProcess = params.DeleteParams.Key, params.DeleteParams.MaxMessageID
	case WorkflowTypeMerge:
		progress.DlqKey, progress.MaxMessageIDToProcess = params.MergeParams.Key, params.MergeParams.MaxMessageID
		if err := validateAndSetMergeDefaults(&params.MergeParams); err != nil {
			return err
		}
		pageSize = params.MergeParams.BatchSize
	default:
		return temporal.NewNonRetryableApplicationError(params.WorkflowType, errorTypeInvalidWorkflowType, nil)
	}
	readContext := segmentReadContext(ctx)
	var segments []historydlq.SegmentRef
	err := workflow.ExecuteActivity(readContext, discoverSegmentsActivityName, SegmentJobParams{
		Key: progress.DlqKey, MaxMessageID: progress.MaxMessageIDToProcess,
	}).Get(ctx, &segments)
	if err != nil {
		return err
	}
	firstError := c.processQueueV2Job(ctx, params, &progress)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if temporal.IsCanceledError(firstError) {
		return firstError
	}
	for _, segment := range segments {
		request := historydlq.ListEntriesRequest{Segment: segment, InclusiveMaxMessageID: progress.MaxMessageIDToProcess, PageSize: pageSize}
		if params.WorkflowType == WorkflowTypeDelete {
			err = c.purgeSegment(ctx, request, &progress)
		} else {
			err = c.mergeSegment(ctx, request, &progress)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if temporal.IsCanceledError(err) {
			return err
		}
		if firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (c *workerComponent) processQueueV2Job(ctx workflow.Context, params WorkflowParams, progress *ProgressQueryResponse) error {
	if params.WorkflowType == WorkflowTypeMerge {
		return c.mergeTasks(ctx, params.MergeParams, &progress.LastProcessedMessageID, &progress.NumberOfMessagesProcessed)
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: primitives.DLQActivityTQ, StartToCloseTimeout: deleteTasksActivityTimeout, RetryPolicy: deleteActivityRetryPolicy,
	})
	var response historyservice.DeleteDLQTasksResponse
	if err := workflow.ExecuteActivity(ctx, deleteTasksIfPresentActivityName, params.DeleteParams).Get(ctx, &response); err != nil {
		return err
	}
	progress.NumberOfMessagesProcessed += response.MessagesDeleted
	progress.LastProcessedMessageID = params.DeleteParams.MaxMessageID
	return nil
}

func (c *workerComponent) deleteTasksIfPresent(ctx context.Context, params DeleteParams) (*historyservice.DeleteDLQTasksResponse, error) {
	response, err := c.deleteTasks(ctx, params)
	if _, absent := errors.AsType[*serviceerror.NotFound](err); absent {
		return &historyservice.DeleteDLQTasksResponse{}, nil
	}
	return response, err
}

func segmentReadContext(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: primitives.DLQActivityTQ, StartToCloseTimeout: mergeTasksActivityTimeout, RetryPolicy: mergeActivityRetryPolicy,
	})
}

func recordSegmentProgress(progress *ProgressQueryResponse, refs []historydlq.MessageRef, resolved int64, failures []MergeEntryFailure) {
	progress.NumberOfMessagesProcessed += resolved
	failedKeys := make(map[string]bool, len(failures))
	for _, failure := range failures {
		failedKeys[failure.Ref.EntryKey] = true
	}
	for _, ref := range refs {
		if !failedKeys[ref.EntryKey] {
			progress.LastProcessedMessageID = max(progress.LastProcessedMessageID, ref.MessageID)
		}
	}
}

type mergeFailures struct {
	count int
	first string
}

func (f *mergeFailures) add(ref historydlq.MessageRef, message string) {
	f.count++
	if f.count == 1 {
		f.first = fmt.Sprintf("%s: %s", ref.EntryKey, message)
	}
}

func (f mergeFailures) err() error {
	if f.count == 0 {
		return nil
	}
	return temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("history DLQ merge left %d entries unresolved; first failure %s", f.count, f.first), errorTypeUnresolvedEntries, nil,
	)
}

func (c *workerComponent) mergeSegment(ctx workflow.Context, request historydlq.ListEntriesRequest, progress *ProgressQueryResponse) error {
	var unresolved mergeFailures
	for {
		var page historydlq.EntryPage
		if err := workflow.ExecuteActivity(segmentReadContext(ctx), listSegmentEntriesActivityName, request).Get(ctx, &page); err != nil {
			return err
		}
		if err := c.mergePage(ctx, request.Segment, page.Refs, progress, &unresolved); err != nil {
			return err
		}
		if len(page.NextPageToken) == 0 {
			return unresolved.err()
		}
		request.NextPageToken = page.NextPageToken
	}
}

func (c *workerComponent) mergePage(
	ctx workflow.Context, segment historydlq.SegmentRef, refs []historydlq.MessageRef,
	progress *ProgressQueryResponse, unresolved *mergeFailures,
) error {
	processingContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: primitives.DLQActivityTQ, StartToCloseTimeout: deleteTasksActivityTimeout,
		// Workflow retries charge every processing attempt to the pending entries.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	pending := refs
	delay := mergeActivityRetryPolicy.InitialInterval
	for attempt := int32(1); len(pending) > 0; attempt++ {
		var result MergeEntriesResult
		err := workflow.ExecuteActivity(processingContext, mergeEntriesActivityName, MergeEntriesParams{Segment: segment, Refs: pending}).Get(ctx, &result)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if temporal.IsCanceledError(err) {
			return err
		}
		if err != nil {
			applicationError, permanent := errors.AsType[*temporal.ApplicationError](err)
			if (permanent && applicationError.NonRetryable()) || attempt == mergeActivityRetryPolicy.MaximumAttempts {
				for _, ref := range pending {
					unresolved.add(ref, err.Error())
				}
				return nil
			}
		} else {
			recordSegmentProgress(progress, pending, result.ResolvedCount, result.Failures)
			pending = unresolved.retryableRefs(result.Failures, attempt == mergeActivityRetryPolicy.MaximumAttempts)
		}
		if len(pending) > 0 {
			if err := workflow.Sleep(ctx, delay); err != nil {
				return err
			}
			delay = time.Duration(float64(delay) * mergeActivityRetryPolicy.BackoffCoefficient)
		}
	}
	return nil
}

func (f *mergeFailures) retryableRefs(failures []MergeEntryFailure, exhausted bool) []historydlq.MessageRef {
	var refs []historydlq.MessageRef
	for _, failure := range failures {
		if failure.NonRetryable || exhausted {
			f.add(failure.Ref, failure.Error)
		} else {
			refs = append(refs, failure.Ref)
		}
	}
	return refs
}
