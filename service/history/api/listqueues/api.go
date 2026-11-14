// Package listqueues implements the HistoryService ListQueues API.
package listqueues

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api/historydlq"
)

func Invoke(ctx context.Context, store *historydlq.Store, req *historyservice.ListQueuesRequest) (*historyservice.ListQueuesResponse, error) {
	return store.ListQueues(ctx, req)
}
