// Package getdlqtasks implements the HistoryService GetDLQTasks API.
package getdlqtasks

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api/historydlq"
)

func Invoke(ctx context.Context, store *historydlq.Store, req *historyservice.GetDLQTasksRequest) (*historyservice.GetDLQTasksResponse, error) {
	return store.GetDLQTasks(ctx, req)
}
