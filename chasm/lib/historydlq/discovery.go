package historydlq

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/primitives"
)

// DiscoverSegments returns visibility candidates; absence is not proof of an empty queue.
func DiscoverSegments(ctx context.Context, visibilityManager chasm.VisibilityManager, queue QueueKey, inclusiveMaxMessageID int64) ([]SegmentRef, error) {
	if err := validateQueue(queue); err != nil {
		return nil, err
	}
	if inclusiveMaxMessageID < 0 {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a nonnegative message bound")
	}
	quote := func(value string) string { return sqlparser.String(sqlparser.NewStrVal([]byte(value))) }
	query := fmt.Sprintf("%s = %d AND %s = %s AND %s = %s", categorySearchAttribute.Value(0).Alias, queue.CategoryID, sourceSearchAttribute.Value("").Alias, quote(queue.SourceCluster), targetSearchAttribute.Value("").Alias, quote(queue.TargetCluster))
	query += fmt.Sprintf(" AND %s >= 0 AND %s <= %d", minMessageIDSearchAttribute.Value(0).Alias, minMessageIDSearchAttribute.Value(0).Alias, inclusiveMaxMessageID)
	request := &chasm.ListExecutionsRequest{NamespaceName: primitives.SystemLocalNamespace, Query: query, PageSize: 1000}
	found := make(map[SegmentRef]struct{})
	tokens := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := visibilityManager.ListExecutions(ctx, reflect.TypeFor[*Segment](), request)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, serviceerror.NewDataLoss("history DLQ visibility returned no response")
		}
		for _, execution := range response.GetExecutions() {
			id := execution.GetBusinessId()
			suffix, ok := strings.CutPrefix(id, "historydlq:segment:v0:")
			decoded, err := hex.DecodeString(suffix)
			if !ok || err != nil || len(decoded) != 32 || execution.GetRunId() == "" {
				return nil, serviceerror.NewDataLoss("history DLQ visibility returned an invalid segment identity")
			}
			found[SegmentRef{Queue: queue, BusinessID: id, RunID: execution.GetRunId()}] = struct{}{}
		}
		if len(response.GetNextPageToken()) == 0 {
			break
		}
		token := string(response.GetNextPageToken())
		if _, ok := tokens[token]; ok {
			return nil, serviceerror.NewDataLoss("history DLQ visibility repeated a page token")
		}
		tokens[token] = struct{}{}
		request.NextPageToken = append([]byte(nil), response.GetNextPageToken()...)
	}
	var refs []SegmentRef
	for ref := range found {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b SegmentRef) int {
		if order := cmp.Compare(a.BusinessID, b.BusinessID); order != 0 {
			return order
		}
		return cmp.Compare(a.RunID, b.RunID)
	})
	return refs, nil
}
