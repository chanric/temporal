package historydlq

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
)

func (s *Store) readQueueV2Tasks(ctx context.Context, queueKey persistence.QueueKey, inclusiveMaxMessageID int64) ([]persistence.RawHistoryTask, error) {
	var tasks []persistence.RawHistoryTask
	request := &persistence.ReadRawTasksRequest{QueueKey: queueKey, PageSize: 256}
	tokens := map[string]struct{}{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := s.queueV2Manager.ReadRawTasks(ctx, request)
		if err != nil {
			if _, ok := errors.AsType[*serviceerror.NotFound](err); ok && len(request.NextPageToken) == 0 {
				break
			}
			return nil, queueV2ReadError(err)
		}
		if page == nil {
			return nil, serviceerror.NewDataLoss("history DLQ legacy read returned no response")
		}
		for _, task := range page.Tasks {
			if task.MessageMetadata.ID > inclusiveMaxMessageID {
				return tasks, nil
			}
			tasks = append(tasks, task)
		}
		if len(page.NextPageToken) == 0 {
			break
		}
		token := string(page.NextPageToken)
		if _, ok := tokens[token]; ok {
			return nil, serviceerror.NewDataLoss("history DLQ legacy read repeated a page token")
		}
		tokens[token] = struct{}{}
		request.NextPageToken = append([]byte(nil), page.NextPageToken...)
	}

	return tasks, nil
}

func (s *Store) readQueueV2Page(ctx context.Context, queue persistence.QueueKey, req *historyservice.GetDLQTasksRequest, inclusiveMaxMessageID int64) (*historyservice.GetDLQTasksResponse, error) {
	page, err := s.queueV2Manager.ReadRawTasks(ctx, &persistence.ReadRawTasksRequest{QueueKey: queue, PageSize: int(req.PageSize), NextPageToken: req.NextPageToken})
	if err != nil {
		if _, absent := errors.AsType[*serviceerror.NotFound](err); absent && len(req.NextPageToken) == 0 {
			return &historyservice.GetDLQTasksResponse{}, nil
		}
		return nil, queueV2ReadError(err)
	}
	response := &historyservice.GetDLQTasksResponse{DlqTasks: make([]*commonspb.HistoryDLQTask, 0, len(page.Tasks)), NextPageToken: page.NextPageToken}
	for _, task := range page.Tasks {
		if task.MessageMetadata.ID > inclusiveMaxMessageID {
			response.NextPageToken = nil
			break
		}
		response.DlqTasks = append(response.DlqTasks, queueV2TaskToProto(task))
	}
	return response, nil
}

func queueV2ReadError(err error) error {
	if errors.Is(err, persistence.ErrReadTasksNonPositivePageSize) {
		return consts.ErrInvalidPageSize
	}
	return serviceerror.NewUnavailablef("GetDLQTasks failed. Error: %v", err)
}

func queueV2TaskToProto(task persistence.RawHistoryTask) *commonspb.HistoryDLQTask {
	return &commonspb.HistoryDLQTask{Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: task.MessageMetadata.ID}, Payload: &commonspb.HistoryTask{ShardId: task.Payload.ShardId, Blob: task.Payload.Blob}}
}
