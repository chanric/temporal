package dlq

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/headers"
)

type testPurgeClient struct {
	segmentClient
	resolve func(context.Context, historydlq.ResolveEntriesRequest) error
}

func (c testPurgeClient) ResolveEntries(ctx context.Context, req historydlq.ResolveEntriesRequest) error {
	return c.resolve(ctx, req)
}

func TestPurgeEntriesBatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		keys       []string
		resolved   []string
		batchSizes []int
	}{
		{name: "empty"},
		{name: "one", keys: []string{"a"}, resolved: []string{"a"}, batchSizes: []int{1}},
		{
			name:       "duplicates and absent",
			keys:       []string{"a", "absent", "a", "b"},
			resolved:   []string{"a", "absent", "b"},
			batchSizes: []int{3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, segment := segmentJobFixture(t)
			var batchSizes []int
			component := &workerComponent{
				currentClusterName: "target",
				chasmClient: testPurgeClient{resolve: func(ctx context.Context, req historydlq.ResolveEntriesRequest) error {
					require.Equal(t, headers.CallerTypePreemptable, headers.GetCallerInfo(ctx).CallerType)
					require.Equal(t, segment, req.Segment)
					require.Equal(t, historydlqpb.RESOLUTION_PURGED, req.Resolution)
					batchSizes = append(batchSizes, len(req.EntryKeys))
					return nil
				}},
			}
			result, err := component.purgeEntries(headers.SetCallerType(t.Context(), headers.CallerTypePreemptable), PurgeEntriesParams{
				Segment: segment, EntryKeys: tc.keys,
			})
			require.NoError(t, err)
			require.Equal(t, PurgeEntriesResult{ResolvedEntryKeys: tc.resolved}, result)
			require.Equal(t, tc.batchSizes, batchSizes)
		})
	}
}

func TestPurgeEntriesChunkRetry(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	var keys []string
	for i := range historydlq.MaxResolveBatchSize + 1 {
		keys = append(keys, fmt.Sprintf("entry-%d", i))
	}
	var requests [][]string
	fail := true
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testPurgeClient{resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
			requests = append(requests, req.EntryKeys)
			if len(req.EntryKeys) == 1 && fail {
				fail = false
				return serviceerror.NewUnavailable("resolve response lost")
			}
			return nil
		}},
	}
	params := PurgeEntriesParams{Segment: segment, EntryKeys: keys}
	result, err := component.purgeEntries(t.Context(), params)
	require.ErrorAs(t, err, new(*serviceerror.Unavailable))
	require.Equal(t, PurgeEntriesResult{}, result)
	result, err = component.purgeEntries(t.Context(), params)
	require.NoError(t, err)
	require.Equal(t, PurgeEntriesResult{ResolvedEntryKeys: keys}, result)
	require.Equal(t, [][]string{
		keys[:historydlq.MaxResolveBatchSize], keys[historydlq.MaxResolveBatchSize:],
		keys[:historydlq.MaxResolveBatchSize], keys[historydlq.MaxResolveBatchSize:],
	}, requests)
}

func TestPurgeEntriesInvalidBeforeMutation(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	var keys []string
	for i := range historydlq.MaxResolveBatchSize {
		keys = append(keys, fmt.Sprintf("entry-%d", i))
	}
	keys = append(keys, "")
	component := &workerComponent{currentClusterName: "target"}
	_, err := component.purgeEntries(t.Context(), PurgeEntriesParams{Segment: segment, EntryKeys: keys})
	var applicationError *temporal.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	require.True(t, applicationError.NonRetryable())
	segment.Queue.TargetCluster = "remote"
	_, err = component.purgeEntries(t.Context(), PurgeEntriesParams{Segment: segment, EntryKeys: []string{"a"}})
	require.ErrorAs(t, err, &applicationError)
	require.True(t, applicationError.NonRetryable())
}

func TestPurgeEntriesMissingRun(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testPurgeClient{resolve: func(context.Context, historydlq.ResolveEntriesRequest) error {
			return serviceerror.NewNotFound("exact segment run missing")
		}},
	}
	result, err := component.purgeEntries(t.Context(), PurgeEntriesParams{Segment: segment, EntryKeys: []string{"a"}})
	var applicationError *temporal.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	require.True(t, applicationError.NonRetryable())
	require.Equal(t, PurgeEntriesResult{}, result)
}
