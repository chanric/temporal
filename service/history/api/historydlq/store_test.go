package historydlq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type queueReader struct {
	persistence.HistoryTaskQueueManager
	read func(context.Context, *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error)
}

func (r queueReader) ReadRawTasks(ctx context.Context, req *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
	return r.read(ctx, req)
}

type metadataClient struct {
	fullRead func(context.Context, dlq.ReadSegmentMessagesRequest) (dlq.MessagePage, error)
	list     func(context.Context, dlq.ListEntriesRequest) (dlq.EntryPage, error)
	read     func(context.Context, dlq.MessageRef) (dlq.StoredMessage, error)
}

func (c metadataClient) ListEntries(ctx context.Context, req dlq.ListEntriesRequest) (dlq.EntryPage, error) {
	return c.list(ctx, req)
}
func (c metadataClient) ReadMessage(ctx context.Context, ref dlq.MessageRef) (dlq.StoredMessage, error) {
	return c.read(ctx, ref)
}

func (c metadataClient) ReadSegmentMessages(ctx context.Context, req dlq.ReadSegmentMessagesRequest) (dlq.MessagePage, error) {
	if c.fullRead != nil {
		return c.fullRead(ctx, req)
	}
	entries, err := c.list(ctx, dlq.ListEntriesRequest(req))
	if err != nil {
		return dlq.MessagePage{}, err
	}
	page := dlq.MessagePage{NextPageToken: entries.NextPageToken}
	for _, ref := range entries.Refs {
		if ref.MessageID > req.InclusiveMaxMessageID {
			continue
		}
		message, err := c.read(ctx, ref)
		if err != nil {
			return dlq.MessagePage{}, err
		}
		page.Messages = append(page.Messages, message)
	}
	return page, nil
}

func readFixture(t *testing.T) (*Store, *historyservice.GetDLQTasksRequest, []dlq.MessageRef) {
	t.Helper()
	queue := dlq.QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"}
	refs := []dlq.MessageRef{}
	rows := []*chasmspb.VisibilityExecutionInfo{}
	for i := range 3 {
		identity := []dlq.TaskIdentity{
			{SourceShardID: 1, TaskID: 1, NamespaceID: "namespace-9", ArchetypeID: 9, BusinessID: "business-9"},
			{SourceShardID: 1, TaskID: 2, NamespaceID: "namespace-0", ArchetypeID: 0, BusinessID: "business-0"},
			{SourceShardID: 2, TaskID: 3, NamespaceID: "namespace-0", ArchetypeID: 0, BusinessID: "business-0"},
		}[i]
		businessID, err := dlq.SegmentBusinessID(queue, identity)
		require.NoError(t, err)
		ref := dlq.MessageRef{Queue: queue, BusinessID: businessID, RunID: fmt.Sprintf("run%d", i), EntryKey: fmt.Sprintf("entry%d", i), MessageID: 10, EnqueuedAt: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
		refs = append(refs, ref)
		rows = append(rows, &chasmspb.VisibilityExecutionInfo{BusinessId: businessID, RunId: ref.RunID})
	}
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{Executions: rows}, nil).AnyTimes()
	blob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("payload")}
	store := &Store{readCHASM: dynamicconfig.GetBoolPropertyFn(true), categoryRegistry: tasks.NewDefaultTaskCategoryRegistry(), visibilityManager: vis,
		queueV2Manager: queueReader{read: func(context.Context, *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
			return &persistence.ReadRawTasksResponse{Tasks: []persistence.RawHistoryTask{{MessageMetadata: persistence.MessageMetadata{ID: 10}, Payload: &persistencespb.HistoryTask{ShardId: 100, Blob: blob}}}}, nil
		}},
		chasmClient: metadataClient{list: func(_ context.Context, req dlq.ListEntriesRequest) (dlq.EntryPage, error) {
			if req.InclusiveMaxMessageID < 0 {
				return dlq.EntryPage{}, fmt.Errorf("unexpected bound: %d", req.InclusiveMaxMessageID)
			}
			for _, ref := range refs {
				if ref.BusinessID == req.Segment.BusinessID {
					return dlq.EntryPage{Refs: []dlq.MessageRef{ref}}, nil
				}
			}
			return dlq.EntryPage{}, nil
		}, read: func(_ context.Context, ref dlq.MessageRef) (dlq.StoredMessage, error) {
			return dlq.StoredMessage{Ref: ref, Task: &commonspb.HistoryTask{ShardId: int32(ref.EntryKey[len(ref.EntryKey)-1]-'0') + 1, Blob: blob}}, nil
		}},
	}
	req := &historyservice.GetDLQTasksRequest{DlqKey: &commonspb.HistoryDLQKey{TaskCategory: 1, SourceCluster: "source", TargetCluster: "target"}, PageSize: 1}
	return store, req, refs
}

func TestMixedStoreReadOrdering(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	req.PageSize = 10
	blob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("payload")}
	store.queueV2Manager = queueReader{read: func(context.Context, *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
		return &persistence.ReadRawTasksResponse{Tasks: []persistence.RawHistoryTask{
			{MessageMetadata: persistence.MessageMetadata{ID: 8}, Payload: &persistencespb.HistoryTask{ShardId: 100, Blob: blob}},
			{MessageMetadata: persistence.MessageMetadata{ID: 12}, Payload: &persistencespb.HistoryTask{ShardId: 101, Blob: blob}},
		}}, nil
	}}
	client := store.chasmClient.(metadataClient)
	list := client.list
	client.list = func(ctx context.Context, r dlq.ListEntriesRequest) (dlq.EntryPage, error) {
		page, err := list(ctx, r)
		for i, ref := range page.Refs {
			page.Refs[i].MessageID = []int64{9, 11, 10}[ref.EntryKey[len(ref.EntryKey)-1]-'0']
		}
		return page, err
	}
	store.chasmClient = client
	response, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	want := &historyservice.GetDLQTasksResponse{}
	for _, row := range []struct {
		id    int64
		shard int32
	}{{8, 100}, {9, 1}, {10, 3}, {11, 2}, {12, 101}} {
		want.DlqTasks = append(want.DlqTasks, &commonspb.HistoryDLQTask{Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: row.id}, Payload: &commonspb.HistoryTask{ShardId: row.shard, Blob: blob}})
	}
	protorequire.ProtoEqual(t, want, response)
}

func TestPaginationTiesAcrossBuckets(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	var got []*commonspb.HistoryDLQTask
	for i := 0; i < 5; i++ {
		response, err := store.GetDLQTasks(t.Context(), req)
		require.NoError(t, err)
		got = append(got, response.DlqTasks...)
		if len(response.NextPageToken) == 0 {
			break
		}
		req.NextPageToken = response.NextPageToken
		req.PageSize = 2
	}
	require.Len(t, got, 4)
	seen := map[int32]bool{}
	for _, row := range got {
		require.Equal(t, int64(10), row.Metadata.MessageId)
		require.False(t, seen[row.Payload.ShardId])
		seen[row.Payload.ShardId] = true
	}
}

func TestReadTokenScope(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*historyservice.GetDLQTasksRequest){
		"queue": func(r *historyservice.GetDLQTasksRequest) { r.DlqKey.SourceCluster = "other" },

		"malformed marked": func(r *historyservice.GetDLQTasksRequest) { r.NextPageToken = []byte("historydlq:read:{bad") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, req, _ := readFixture(t)
			response, err := store.GetDLQTasks(t.Context(), req)
			require.NoError(t, err)
			req.NextPageToken = response.NextPageToken
			change(req)
			_, err = store.GetDLQTasks(t.Context(), req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestResolvedEntryBetweenPages(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	first, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	req.NextPageToken = first.NextPageToken
	client := store.chasmClient.(metadataClient)
	client.list = func(context.Context, dlq.ListEntriesRequest) (dlq.EntryPage, error) { return dlq.EntryPage{}, nil }
	store.chasmClient = client
	second, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Empty(t, second.DlqTasks)
	require.Empty(t, second.NextPageToken)
}

func TestFullMessagePageDoesNotNeedReferenceHydration(t *testing.T) {
	t.Parallel()
	store, req, refs := readFixture(t)
	req.PageSize = 10
	client := store.chasmClient.(metadataClient)
	read := client.read
	client.list = nil
	client.read = nil
	client.fullRead = func(ctx context.Context, r dlq.ReadSegmentMessagesRequest) (dlq.MessagePage, error) {
		for _, ref := range refs {
			if ref.BusinessID == r.Segment.BusinessID {
				message, err := read(ctx, ref)
				return dlq.MessagePage{Messages: []dlq.StoredMessage{message}}, err
			}
		}
		return dlq.MessagePage{}, nil
	}
	store.chasmClient = client
	response, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, response.DlqTasks, 4)
}

func TestMissingSegmentDuringRead(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	req.PageSize = 10
	client := store.chasmClient.(metadataClient)
	list := client.list
	var mu sync.Mutex
	missing := false
	client.read = func(context.Context, dlq.MessageRef) (dlq.StoredMessage, error) {
		mu.Lock()
		missing = true
		mu.Unlock()
		return dlq.StoredMessage{}, serviceerror.NewNotFound("missing exact run")
	}
	client.list = func(ctx context.Context, req dlq.ListEntriesRequest) (dlq.EntryPage, error) {
		mu.Lock()
		gone := missing
		mu.Unlock()
		if gone {
			return dlq.EntryPage{}, serviceerror.NewNotFound("missing exact run")
		}
		return list(ctx, req)
	}
	store.chasmClient = client
	_, err := store.GetDLQTasks(t.Context(), req)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestPendingPayloadCorruption(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	req.PageSize = 10
	client := store.chasmClient.(metadataClient)
	client.read = func(context.Context, dlq.MessageRef) (dlq.StoredMessage, error) {
		return dlq.StoredMessage{}, serviceerror.NewDataLoss("missing DATA")
	}
	store.chasmClient = client
	_, err := store.GetDLQTasks(t.Context(), req)
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}

func TestLegacyContinuation(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	req.NextPageToken = []byte("raw legacy cursor")
	store.chasmClient = nil
	store.visibilityManager = nil
	store.queueV2Manager = queueReader{read: func(_ context.Context, r *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
		require.Equal(t, req.NextPageToken, r.NextPageToken)
		require.Equal(t, int(req.PageSize), r.PageSize)
		return &persistence.ReadRawTasksResponse{NextPageToken: []byte("next legacy")}, nil
	}}
	response, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, []byte("next legacy"), response.NextPageToken)
}

func TestMixedStoreFailureAndCancellation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"queue", "metadata", "unexplained not found", "cancelled", "absent legacy"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			store, req, _ := readFixture(t)
			req.PageSize = 10
			expected := errors.New("injected")
			ctx := t.Context()
			switch kind {
			case "queue":
				store.queueV2Manager = queueReader{read: func(context.Context, *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
					return nil, expected
				}}
			case "metadata":
				client := store.chasmClient.(metadataClient)
				client.list = func(context.Context, dlq.ListEntriesRequest) (dlq.EntryPage, error) { return dlq.EntryPage{}, expected }
				store.chasmClient = client
			case "unexplained not found":
				client := store.chasmClient.(metadataClient)
				client.read = func(context.Context, dlq.MessageRef) (dlq.StoredMessage, error) {
					return dlq.StoredMessage{}, serviceerror.NewNotFound("entry unchanged")
				}
				store.chasmClient = client
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "absent legacy":
				store.queueV2Manager = queueReader{read: func(context.Context, *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
					return nil, persistence.NewQueueNotFoundError(persistence.QueueTypeHistoryDLQ, "absent")
				}}
			default:
				t.Fatalf("unknown test case: %s", kind)
			}
			response, err := store.GetDLQTasks(ctx, req)
			if kind == "absent legacy" {
				require.NoError(t, err)
				require.Len(t, response.DlqTasks, 3)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestLegacyActivityReadStaysQueueV2(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	ctx := headers.SetCallerType(t.Context(), headers.CallerTypePreemptable)
	store.visibilityManager = nil
	store.chasmClient = nil
	response, err := store.GetDLQTasks(ctx, req)
	require.NoError(t, err)
	require.Len(t, response.DlqTasks, 1)
	require.Equal(t, int32(100), response.DlqTasks[0].Payload.ShardId)
}

func TestMixedReadFollowsBackendPages(t *testing.T) {
	t.Parallel()
	store, req, refs := readFixture(t)
	req.PageSize = 10
	store.queueV2Manager = queueReader{read: func(_ context.Context, r *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
		switch string(r.NextPageToken) {
		case "":
			return &persistence.ReadRawTasksResponse{NextPageToken: []byte("second")}, nil
		case "second":
			return &persistence.ReadRawTasksResponse{}, nil
		default:
			return nil, errors.New("unexpected legacy token")
		}
	}}
	client := store.chasmClient.(metadataClient)
	list := client.list
	client.list = func(ctx context.Context, r dlq.ListEntriesRequest) (dlq.EntryPage, error) {
		if r.Segment.BusinessID != refs[0].BusinessID {
			return dlq.EntryPage{}, nil
		}
		if len(r.NextPageToken) == 0 {
			return dlq.EntryPage{NextPageToken: []byte("second")}, nil
		}
		return list(ctx, r)
	}
	store.chasmClient = client
	response, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, response.DlqTasks, 1)
}

func TestMixedReadBoundsConcurrentSegmentRPCs(t *testing.T) {
	t.Parallel()
	store, req, refs := readFixture(t)
	rows := make([]*chasmspb.VisibilityExecutionInfo, 20)
	for i := range rows {
		id, err := dlq.SegmentBusinessID(refs[0].Queue, dlq.TaskIdentity{SourceShardID: int32(i + 1), TaskID: 1})
		require.NoError(t, err)
		rows[i] = &chasmspb.VisibilityExecutionInfo{BusinessId: id, RunId: fmt.Sprintf("run%d", i)}
	}
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{Executions: rows}, nil)
	store.visibilityManager = vis
	started := make(chan struct{}, 20)
	client := store.chasmClient.(metadataClient)
	client.list = func(ctx context.Context, _ dlq.ListEntriesRequest) (dlq.EntryPage, error) {
		started <- struct{}{}
		<-ctx.Done()
		return dlq.EntryPage{}, ctx.Err()
	}
	store.chasmClient = client
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := store.GetDLQTasks(ctx, req); result <- err }()
	for range 8 {
		select {
		case <-started:
		case <-t.Context().Done():
			t.Fatal("metadata calls did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("more than eight metadata RPCs started")
	default:
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-t.Context().Done():
		t.Fatal("read did not honor cancellation")
	}
}

func TestHistoryReadCHASMDynamicConfig(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	dcClient := dynamicconfig.NewMemoryClient()
	dc := dynamicconfig.NewCollection(dcClient, log.NewNoopLogger())
	store.readCHASM = dynamicconfig.HistoryTaskDLQReadCHASM.Get(dc)
	legacy, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, legacy.DlqTasks, 1)
	t.Cleanup(dcClient.OverrideSetting(dynamicconfig.HistoryTaskDLQReadCHASM, true))
	req.PageSize = 10
	mixed, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, mixed.DlqTasks, 4)
}

func TestHistoryReadCHASMRejectsCursorAfterDisable(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	first, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.NotEmpty(t, first.NextPageToken)
	req.NextPageToken = first.NextPageToken
	store.readCHASM = dynamicconfig.GetBoolPropertyFn(false)
	_, err = store.GetDLQTasks(t.Context(), req)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
}

func TestQueueV2ReaderDoesNotUseCHASM(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	store.chasmClient = nil
	store.visibilityManager = nil
	queueKey := persistence.QueueKey{QueueType: persistence.QueueTypeHistoryDLQ, Category: tasks.CategoryTransfer, SourceCluster: req.DlqKey.SourceCluster, TargetCluster: req.DlqKey.TargetCluster}
	want, err := store.queueV2Manager.ReadRawTasks(t.Context(), &persistence.ReadRawTasksRequest{QueueKey: queueKey, PageSize: 256})
	require.NoError(t, err)
	got, err := store.readQueueV2Tasks(t.Context(), queueKey, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, want.Tasks, got)
}

func TestCHASMReaderDoesNotUseQueueV2(t *testing.T) {
	t.Parallel()
	store, _, refs := readFixture(t)
	store.queueV2Manager = nil
	got, err := store.readCHASMMessages(t.Context(), refs[0].Queue, math.MaxInt64)
	require.NoError(t, err)
	var gotRefs []dlq.MessageRef
	for _, message := range got {
		gotRefs = append(gotRefs, message.Ref)
	}
	require.ElementsMatch(t, refs, gotRefs)
}

func TestReadInclusiveMaxMessageID(t *testing.T) {
	t.Parallel()
	for _, bound := range []int64{0, 9, 10} {
		t.Run(fmt.Sprint(bound), func(t *testing.T) {
			t.Parallel()
			store, req, _ := readFixture(t)
			req.PageSize = 10
			req.InclusiveMaxTaskMetadata = &commonspb.HistoryDLQTaskMetadata{MessageId: bound}
			response, err := store.GetDLQTasks(t.Context(), req)
			require.NoError(t, err)
			if bound < 10 {
				require.Empty(t, response.DlqTasks)
			} else {
				require.Len(t, response.DlqTasks, 4)
			}
		})
	}
}

func TestReadCursorBindsMessageBound(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	response, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	req.NextPageToken = response.NextPageToken
	req.InclusiveMaxTaskMetadata = &commonspb.HistoryDLQTaskMetadata{MessageId: 10}
	_, err = store.GetDLQTasks(t.Context(), req)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
}

func TestDiscoveryUsesRequestedMessageBound(t *testing.T) {
	t.Parallel()
	store, req, _ := readFixture(t)
	req.InclusiveMaxTaskMetadata = &commonspb.HistoryDLQTaskMetadata{MessageId: 9}
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ reflect.Type, r *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
		require.Contains(t, r.Query, "HistoryDLQMinMessageID >= 0")
		require.Contains(t, r.Query, "HistoryDLQMinMessageID <= 9")
		return &visibilityservice.ListChasmExecutionsResponse{}, nil
	})
	store.visibilityManager = vis
	response, err := store.GetDLQTasks(t.Context(), req)
	require.NoError(t, err)
	require.Empty(t, response.DlqTasks)
}

func TestQueueV2MissingQueuePage(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"missing first page", "missing continuation", "backend failure"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			store, req, _ := readFixture(t)
			store.chasmClient, store.visibilityManager = nil, nil
			expected := persistence.NewQueueNotFoundError(persistence.QueueTypeHistoryDLQ, "absent")
			if scenario == "missing continuation" {
				req.NextPageToken = []byte("next")
			}
			if scenario == "backend failure" {
				expected = serviceerror.NewUnavailable("database unavailable")
			}
			store.queueV2Manager = queueReader{read: func(context.Context, *persistence.ReadRawTasksRequest) (*persistence.ReadRawTasksResponse, error) {
				return nil, expected
			}}
			response, err := store.GetDLQTasks(headers.SetCallerType(t.Context(), headers.CallerTypePreemptable), req)
			if scenario == "missing first page" {
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &historyservice.GetDLQTasksResponse{}, response)
			} else {
				require.ErrorAs(t, err, new(*serviceerror.Unavailable))
			}
		})
	}
}
