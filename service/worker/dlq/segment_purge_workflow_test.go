package dlq

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/primitives"
)

type purgeFixture struct {
	segment    *historydlq.Segment
	ctx        *chasm.MockMutableContext
	ref        historydlq.SegmentRef
	request    historydlq.EnqueueRequest
	refs       []historydlq.MessageRef
	now        time.Time
	emptyBytes int64
}

func newPurgeFixture(t *testing.T) *purgeFixture {
	t.Helper()
	fixture := &purgeFixture{
		now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
		request: historydlq.EnqueueRequest{
			Queue: historydlq.QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"},
			SourceTaskIdentity: historydlq.TaskIdentity{
				SourceShardID: 7, TaskID: 7115,
				NamespaceID: "namespace", BusinessID: "workflow", RunID: "run", ArchetypeID: 1,
			},
			Task: &commonspb.HistoryTask{
				ShardId: 7, Blob: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("task payload")},
			},
		},
	}
	businessID, err := historydlq.SegmentBusinessID(fixture.request.Queue, fixture.request.SourceTaskIdentity)
	require.NoError(t, err)
	fixture.ref = historydlq.SegmentRef{Queue: fixture.request.Queue, BusinessID: businessID, RunID: "segment-run"}
	fixture.ctx = &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return fixture.now },
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{NamespaceID: primitives.SystemNamespaceID, BusinessID: businessID, RunID: fixture.ref.RunID}
		},
	}}
	fixture.segment, err = historydlq.NewSegment(fixture.ctx, fixture.request)
	require.NoError(t, err)
	fixture.emptyBytes = fixture.segment.GetRetainedBytes()
	for i, taskID := range []int64{7115, 7134, 7116} {
		if i == 2 {
			fixture.now = fixture.now.Add(time.Hour)
		}
		request := fixture.request
		request.SourceTaskIdentity.TaskID = taskID
		accepted, err := fixture.segment.Append(fixture.ctx, request)
		require.NoError(t, err)
		fixture.refs = append(fixture.refs, accepted.MessageRef)
	}
	require.Equal(t, fixture.refs[0].MessageID, fixture.refs[1].MessageID)
	return fixture
}

func TestPurgeSegmentPages(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"normal", "lost resolve response", "recreated after lost response"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			fixture := newPurgeFixture(t)
			// Removing DATA makes an accidental payload read fail.
			for _, field := range fixture.segment.Entries {
				field.Get(fixture.ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
			}
			var mu sync.Mutex
			var events []string
			var minima []int64
			lostResponse := false
			component := &workerComponent{
				currentClusterName: "target",
				chasmClient: testPurgeClient{
					segmentClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
						mu.Lock()
						defer mu.Unlock()
						events = append(events, "list")
						return fixture.segment.ListEntries(fixture.ctx, req)
					}},
					resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
						mu.Lock()
						defer mu.Unlock()
						events = append(events, "resolve")
						if err := fixture.segment.ResolveEntries(fixture.ctx, req); err != nil {
							return err
						}
						minima = append(minima, fixture.segment.GetMinMessageId())
						if scenario != "normal" && !lostResponse {
							lostResponse = true
							if scenario == "recreated after lost response" {
								request := fixture.request
								for i, ref := range fixture.refs {
									if ref.EntryKey == req.EntryKeys[0] {
										request.SourceTaskIdentity.TaskID = []int64{7115, 7134, 7116}[i]
									}
								}
								fixture.now = fixture.now.Add(time.Hour)
								accepted, err := fixture.segment.Append(fixture.ctx, request)
								if err != nil {
									return err
								}
								fixture.segment.Entries[accepted.MessageRef.EntryKey].Get(fixture.ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
							}
							return serviceerror.NewUnavailable("resolve response lost")
						}
						return nil
					},
				},
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			component.RegisterActivities(env)
			env.ExecuteWorkflow(component.purgeSegment, historydlq.ListEntriesRequest{
				Segment: fixture.ref, InclusiveMaxMessageID: math.MaxInt64, PageSize: 1,
			})
			require.NoError(t, env.GetWorkflowError())
			var processed int64
			require.NoError(t, env.GetWorkflowResult(&processed))
			require.Equal(t, int64(3), processed)
			mu.Lock()
			defer mu.Unlock()
			expectedEvents := []string{"list", "resolve", "list", "resolve", "list", "resolve"}
			expectedMinima := []int64{fixture.refs[0].MessageID, fixture.refs[2].MessageID, -1}
			if scenario != "normal" {
				expectedEvents = []string{"list", "resolve", "resolve", "list", "resolve", "list", "resolve"}
				expectedMinima = []int64{fixture.refs[0].MessageID, fixture.refs[0].MessageID, fixture.refs[2].MessageID, -1}
			}
			require.Equal(t, expectedEvents, events)
			require.Equal(t, expectedMinima, minima)
			require.Empty(t, fixture.segment.Entries)
			require.Equal(t, fixture.emptyBytes, fixture.segment.GetRetainedBytes())
			require.Equal(t, int64(-1), fixture.segment.GetMinMessageId())
		})
	}
}

func TestPurgeEntriesConcurrentResolution(t *testing.T) {
	t.Parallel()
	fixture := newPurgeFixture(t)
	page, err := fixture.segment.ListEntries(fixture.ctx, historydlq.ListEntriesRequest{
		Segment: fixture.ref, InclusiveMaxMessageID: math.MaxInt64, PageSize: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fixture.segment.ResolveEntries(fixture.ctx, historydlq.ResolveEntriesRequest{
		Segment: fixture.ref, EntryKeys: []string{page.Refs[0].EntryKey}, Resolution: historydlqpb.RESOLUTION_PURGED,
	}))
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testPurgeClient{resolve: func(_ context.Context, req historydlq.ResolveEntriesRequest) error {
			return fixture.segment.ResolveEntries(fixture.ctx, req)
		}},
	}
	keys := []string{page.Refs[0].EntryKey, page.Refs[1].EntryKey}
	result, err := component.purgeEntries(t.Context(), PurgeEntriesParams{Segment: fixture.ref, EntryKeys: keys})
	require.NoError(t, err)
	require.Equal(t, PurgeEntriesResult{ResolvedEntryKeys: keys}, result)
	require.Len(t, fixture.segment.Entries, 1)
	require.Equal(t, fixture.refs[2].MessageID, fixture.segment.GetMinMessageId())
}

func TestPurgeSegmentEmptyPages(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	var mu sync.Mutex
	var events []string
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testPurgeClient{
			segmentClient: testSegmentMetadataClient{list: func(_ context.Context, req historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
				mu.Lock()
				defer mu.Unlock()
				events = append(events, "list:"+string(req.NextPageToken))
				switch string(req.NextPageToken) {
				case "":
					return historydlq.EntryPage{NextPageToken: []byte("a")}, nil
				case "a":
					return historydlq.EntryPage{Refs: []historydlq.MessageRef{{EntryKey: "key"}}, NextPageToken: []byte("b")}, nil
				default:
					return historydlq.EntryPage{}, nil
				}
			}},
			resolve: func(context.Context, historydlq.ResolveEntriesRequest) error {
				mu.Lock()
				defer mu.Unlock()
				events = append(events, "resolve")
				return nil
			},
		},
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component.RegisterActivities(env)
	env.ExecuteWorkflow(component.purgeSegment, historydlq.ListEntriesRequest{Segment: segment, InclusiveMaxMessageID: 10})
	require.NoError(t, env.GetWorkflowError())
	var processed int64
	require.NoError(t, env.GetWorkflowResult(&processed))
	require.Equal(t, int64(1), processed)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"list:", "list:a", "resolve", "list:b"}, events)
}

func TestPurgeSegmentMissingRun(t *testing.T) {
	t.Parallel()
	_, segment := segmentJobFixture(t)
	component := &workerComponent{
		currentClusterName: "target",
		chasmClient: testSegmentMetadataClient{list: func(context.Context, historydlq.ListEntriesRequest) (historydlq.EntryPage, error) {
			return historydlq.EntryPage{}, serviceerror.NewNotFound("exact segment run missing")
		}},
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	component.RegisterActivities(env)
	env.ExecuteWorkflow(component.purgeSegment, historydlq.ListEntriesRequest{Segment: segment, InclusiveMaxMessageID: 10})
	var applicationError *temporal.ApplicationError
	require.ErrorAs(t, env.GetWorkflowError(), &applicationError)
	require.True(t, applicationError.NonRetryable())
}
