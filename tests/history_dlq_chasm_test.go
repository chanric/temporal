package tests

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/historydlq"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lostDLQResponseClient struct {
	historydlqpb.HistoryDLQServiceClient
	accepted *historydlqpb.EnqueueResponse
}

func (c *lostDLQResponseClient) Enqueue(
	ctx context.Context,
	req *historydlqpb.EnqueueRequest,
	opts ...grpc.CallOption,
) (*historydlqpb.EnqueueResponse, error) {
	response, err := c.HistoryDLQServiceClient.Enqueue(ctx, req, opts...)
	if err == nil && c.accepted == nil {
		c.accepted = proto.Clone(response).(*historydlqpb.EnqueueResponse)
		return nil, serviceerror.NewUnavailable("lost successful response")
	}
	return response, err
}

func newHistoryDLQEnqueueRequest(
	t *testing.T,
	env *testcore.TestEnv,
	id int64,
	size int,
) *historydlqpb.EnqueueRequest {
	t.Helper()
	tv := testvars.New(t)
	return newHistoryDLQProducerEnqueueRequest(t, env, id, size, historydlq.TaskIdentity{
		NamespaceID: env.NamespaceID().String(), BusinessID: tv.WorkflowID(), RunID: tv.RunID(),
	})
}

func newHistoryDLQProducerEnqueueRequest(
	t *testing.T,
	env *testcore.TestEnv,
	id int64,
	size int,
	producer historydlq.TaskIdentity,
) *historydlqpb.EnqueueRequest {
	t.Helper()
	task := &tasks.HistoryReplicationTask{
		WorkflowKey:         definition.NewWorkflowKey(producer.NamespaceID, producer.BusinessID, producer.RunID),
		TaskID:              id,
		VisibilityTimestamp: time.Now().UTC(),
		BranchToken: bytes.Repeat([]byte{
			42,
		}, size),
	}
	blob, err := serialization.NewSerializer().SerializeTask(task)
	require.NoError(t, err)
	queue := historydlq.QueueKey{
		CategoryID:    int32(tasks.CategoryReplication.ID()),
		SourceCluster: env.NamespaceID().String(),
		TargetCluster: cluster.TestCurrentClusterName,
	}
	original := historydlq.TaskIdentity{
		SourceShardID: 7,
		TaskID:        id,
		NamespaceID:   task.NamespaceID,
		BusinessID:    task.WorkflowID,
		RunID:         task.RunID,
		ArchetypeID:   producer.ArchetypeID,
	}
	businessID, err := historydlq.SegmentBusinessID(queue, original)
	require.NoError(t, err)
	return &historydlqpb.EnqueueRequest{
		SystemNamespaceId: primitives.SystemNamespaceID,
		BusinessId:        businessID,
		Queue: &commonspb.HistoryDLQKey{
			TaskCategory:  queue.CategoryID,
			SourceCluster: queue.SourceCluster,
			TargetCluster: queue.TargetCluster,
		},
		SourceTaskIdentity: &historydlqpb.TaskIdentity{
			SourceShardId: 7,
			TaskId:        id,
			NamespaceId:   original.NamespaceID,
			BusinessId:    original.BusinessID,
			RunId:         original.RunID,
			ArchetypeId:   original.ArchetypeID,
		},
		Task: &commonspb.HistoryTask{
			ShardId: 7,
			Blob:    blob,
		},
	}
}

func newHistoryDLQTestEnvironment(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	t.Helper()
	return testcore.NewEnv(t, append([]testcore.TestOption{
		testcore.WithDedicatedCluster(),
		testcore.WithHistoryShardCount(4),
		testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, true),
	}, opts...)...)
}

func TestHistoryDLQCHASMSuite(t *testing.T) {
	t.Parallel()
	t.Run("WorkerSegmentDependencies", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t, testcore.WithWorkerService("history DLQ segment job dependencies"))
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		request := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		accepted, err := rpc.Enqueue(t.Context(), request)
		require.NoError(t, err)
		response, err := rpc.ListEntries(t.Context(), &historydlqpb.ListEntriesRequest{
			SystemNamespaceId:     primitives.SystemNamespaceID,
			Segment:               &historydlqpb.SegmentRef{Queue: request.Queue, BusinessId: accepted.Ref.BusinessId, RunId: accepted.Ref.RunId},
			InclusiveMaxMessageId: accepted.Ref.MessageId,
			PageSize:              1,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, accepted.Ref, response.Refs[0])
	})
	t.Run("WriterRollbackKeepsReads", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t, testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, true))
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		client := historydlq.NewClient(env.Logger, rpc)
		req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		accepted, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
		segment := historydlq.SegmentRef{Queue: historydlq.QueueKey{CategoryID: req.Queue.TaskCategory, SourceCluster: req.Queue.SourceCluster, TargetCluster: req.Queue.TargetCluster}, BusinessID: accepted.Ref.BusinessId, RunID: accepted.Ref.RunId}
		page, err := client.ListEntries(t.Context(), historydlq.ListEntriesRequest{Segment: segment, InclusiveMaxMessageID: math.MaxInt64, PageSize: 1})
		require.NoError(t, err)
		require.Len(t, page.Refs, 1)
		require.Equal(t, accepted.Ref.EntryKey, page.Refs[0].EntryKey)
		stats, err := client.DescribeSegment(t.Context(), segment)
		require.NoError(t, err)
		require.Equal(t, historydlq.SegmentStats{PendingCount: 1, LastMessageID: accepted.Ref.MessageId}, stats)

		manager, err := env.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
		require.NoError(t, err)
		t.Cleanup(manager.Close)
		_, err = manager.CreateQueue(t.Context(), &persistence.CreateQueueRequest{QueueKey: persistence.QueueKey{QueueType: persistence.QueueTypeHistoryDLQ, Category: tasks.CategoryReplication, SourceCluster: req.Queue.SourceCluster, TargetCluster: req.Queue.TargetCluster}})
		require.NoError(t, err)
		task, err := serialization.NewSerializer().DeserializeTask(tasks.CategoryReplication, req.Task.Blob)
		require.NoError(t, err)
		_, err = manager.EnqueueTask(t.Context(), &persistence.EnqueueTaskRequest{QueueType: persistence.QueueTypeHistoryDLQ, SourceCluster: req.Queue.SourceCluster, TargetCluster: req.Queue.TargetCluster, SourceShardID: 9, Task: task})
		require.NoError(t, err)
		env.OverrideDynamicConfig(dynamicconfig.HistoryTaskDLQUseCHASM, false)
		legacyTask := &commonspb.HistoryDLQTask{Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: 0}, Payload: &commonspb.HistoryTask{ShardId: 9, Blob: req.Task.Blob}}
		chasmTask := &commonspb.HistoryDLQTask{Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: accepted.Ref.MessageId}, Payload: req.Task}
		await.Require(t.Context(), t, func(t *await.T) {
			response, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: req.Queue, PageSize: 10})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &adminservice.GetDLQTasksResponse{DlqTasks: []*commonspb.HistoryDLQTask{legacyTask, chasmTask}}, response)
		}, 10*time.Second, 100*time.Millisecond)
		bounded, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: req.Queue, PageSize: 10, InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{MessageId: 0}})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &adminservice.GetDLQTasksResponse{DlqTasks: []*commonspb.HistoryDLQTask{legacyTask}}, bounded)
		fullBounded, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: req.Queue, PageSize: 10, InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{MessageId: accepted.Ref.MessageId}})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &adminservice.GetDLQTasksResponse{DlqTasks: []*commonspb.HistoryDLQTask{legacyTask, chasmTask}}, fullBounded)
		first, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: req.Queue, PageSize: 1})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, legacyTask, first.DlqTasks[0])
		require.NotEmpty(t, first.NextPageToken)
		second, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{DlqKey: req.Queue, PageSize: 2, NextPageToken: first.NextPageToken})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &adminservice.GetDLQTasksResponse{DlqTasks: []*commonspb.HistoryDLQTask{chasmTask}}, second)
		legacy, err := env.GetTestCluster().HistoryClient().GetDLQTasks(headers.SetCallerType(t.Context(), headers.CallerTypePreemptable), &historyservice.GetDLQTasksRequest{DlqKey: req.Queue, PageSize: 10})
		require.NoError(t, err)
		raw, err := manager.ReadRawTasks(t.Context(), &persistence.ReadRawTasksRequest{QueueKey: persistence.QueueKey{QueueType: persistence.QueueTypeHistoryDLQ, Category: tasks.CategoryReplication, SourceCluster: req.Queue.SourceCluster, TargetCluster: req.Queue.TargetCluster}, PageSize: 10})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &historyservice.GetDLQTasksResponse{DlqTasks: []*commonspb.HistoryDLQTask{legacyTask}, NextPageToken: raw.NextPageToken}, legacy)
	})
	t.Run("ProducersShareSegment", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		type result struct {
			req      *historydlqpb.EnqueueRequest
			response *historydlqpb.EnqueueResponse
			err      error
		}
		results := make(chan result, 8)
		for i, producer := range []historydlq.TaskIdentity{
			{NamespaceID: "namespace-9", ArchetypeID: 9, BusinessID: "business-9"},
			{NamespaceID: "namespace-0", ArchetypeID: 0, BusinessID: "business-0"},
			{NamespaceID: "namespace-2", ArchetypeID: 2, BusinessID: "business-2"},
			{NamespaceID: "namespace-1", ArchetypeID: 1, BusinessID: "business-1"},
		} {
			for _, id := range []int64{123, 1 << 40} {
				producer.RunID = primitives.NewUUID().String()
				req := newHistoryDLQProducerEnqueueRequest(t, env, id+int64(i), 1024, producer)
				go func() {
					response, err := rpc.Enqueue(t.Context(), req)
					results <- result{req, response, err}
				}()
			}
		}
		segments := make(map[string]string)
		entries := make(map[string]struct{})
		var accepted []result
		for range 8 {
			result := await.Rcv(t, results)
			require.NoError(t, result.err)
			ref := result.response.Ref
			if runID, ok := segments[ref.BusinessId]; ok {
				require.Equal(t, runID, ref.RunId)
			}
			segments[ref.BusinessId] = ref.RunId
			require.NotContains(t, entries, ref.EntryKey)
			entries[ref.EntryKey] = struct{}{}
			accepted = append(accepted, result)
		}
		require.Len(t, segments, 1)
		for businessID := range segments {
			env.CloseShard(primitives.SystemNamespaceID, businessID)
		}
		for _, result := range accepted {
			retry, err := rpc.Enqueue(t.Context(), result.req)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, result.response, retry)
			read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
				SystemNamespaceId: primitives.SystemNamespaceID, Ref: retry.Ref,
			})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &historydlqpb.StoredMessage{
				Ref: retry.Ref, SourceTaskIdentity: result.req.SourceTaskIdentity,
				Task: result.req.Task,
			}, read.Message)
		}
	})
	t.Run("MissingProducerMetadata", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		var segment *historydlqpb.SegmentRef
		var previousEntryKey string
		for _, taskID := range []int64{123, 1 << 40} {
			request := newHistoryDLQProducerEnqueueRequest(t, env, taskID, 1024, historydlq.TaskIdentity{})
			protorequire.ProtoEqual(t, &historydlqpb.TaskIdentity{SourceShardId: 7, TaskId: taskID}, request.SourceTaskIdentity)
			hostingShardID := common.WorkflowIDToHistoryShard(
				primitives.SystemNamespaceID, request.BusinessId, env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
			)
			require.NotEqual(t, request.SourceTaskIdentity.SourceShardId, hostingShardID)
			accepted, err := rpc.Enqueue(t.Context(), request)
			require.NoError(t, err)
			if segment == nil {
				segment = &historydlqpb.SegmentRef{
					Queue:      request.Queue,
					BusinessId: accepted.Ref.BusinessId,
					RunId:      accepted.Ref.RunId,
				}
			} else {
				require.Equal(t, segment.BusinessId, accepted.Ref.BusinessId)
				require.Equal(t, segment.RunId, accepted.Ref.RunId)
				require.NotEqual(t, previousEntryKey, accepted.Ref.EntryKey)
			}
			previousEntryKey = accepted.Ref.EntryKey
			env.CloseShard(primitives.SystemNamespaceID, request.BusinessId)
			retry, err := rpc.Enqueue(t.Context(), request)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, accepted, retry)
			read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
				SystemNamespaceId: primitives.SystemNamespaceID, Ref: accepted.Ref,
			})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &historydlqpb.StoredMessage{
				Ref:                accepted.Ref,
				SourceTaskIdentity: request.SourceTaskIdentity,
				Task:               request.Task,
			}, read.Message)
		}
		stats, err := rpc.DescribeSegment(t.Context(), &historydlqpb.DescribeSegmentRequest{
			SystemNamespaceId: primitives.SystemNamespaceID, Segment: segment,
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), stats.PendingCount)
	})
	t.Run("ExactSegmentRun", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		request := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		accepted, err := rpc.Enqueue(t.Context(), request)
		require.NoError(t, err)
		wrongRef := common.CloneProto(accepted.Ref)
		wrongRef.RunId = primitives.NewUUID().String()
		wrongSegment := &historydlqpb.SegmentRef{
			Queue:      request.Queue,
			BusinessId: accepted.Ref.BusinessId,
			RunId:      wrongRef.RunId,
		}
		env.CloseShard(primitives.SystemNamespaceID, request.BusinessId)
		for _, tc := range []struct {
			name   string
			invoke func(context.Context) error
		}{
			{"ReadMessage", func(ctx context.Context) error {
				_, err := rpc.ReadMessage(ctx, &historydlqpb.ReadMessageRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Ref:               wrongRef,
				})
				return err
			}},
			{"ListEntries", func(ctx context.Context) error {
				_, err := rpc.ListEntries(ctx, &historydlqpb.ListEntriesRequest{
					SystemNamespaceId:     primitives.SystemNamespaceID,
					Segment:               wrongSegment,
					InclusiveMaxMessageId: math.MaxInt64,
					PageSize:              1,
				})
				return err
			}},
			{"ReadSegmentMessages", func(ctx context.Context) error {
				_, err := rpc.ReadSegmentMessages(ctx, &historydlqpb.ReadSegmentMessagesRequest{
					SystemNamespaceId:     primitives.SystemNamespaceID,
					Segment:               wrongSegment,
					InclusiveMaxMessageId: math.MaxInt64,
					PageSize:              1,
				})
				return err
			}},
			{"DescribeSegment", func(ctx context.Context) error {
				_, err := rpc.DescribeSegment(ctx, &historydlqpb.DescribeSegmentRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Segment:           wrongSegment,
				})
				return err
			}},
			{"Resolve", func(ctx context.Context) error {
				_, err := rpc.Resolve(ctx, &historydlqpb.ResolveRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Ref:               wrongRef,
					Resolution:        historydlqpb.RESOLUTION_PURGED,
				})
				return err
			}},
			{"ResolveEntries", func(ctx context.Context) error {
				_, err := rpc.ResolveEntries(ctx, &historydlqpb.ResolveEntriesRequest{
					SystemNamespaceId: primitives.SystemNamespaceID,
					Segment:           wrongSegment,
					EntryKeys:         []string{accepted.Ref.EntryKey},
					Resolution:        historydlqpb.RESOLUTION_PURGED,
				})
				return err
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				require.ErrorAs(t, tc.invoke(t.Context()), new(*serviceerror.NotFound))
				read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
					SystemNamespaceId: primitives.SystemNamespaceID, Ref: accepted.Ref,
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &historydlqpb.StoredMessage{
					Ref:                accepted.Ref,
					SourceTaskIdentity: request.SourceTaskIdentity,
					Task:               request.Task,
				}, read.Message)
			})
		}
	})
	t.Run("DurableEnqueue", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		client := historydlq.NewClient(env.Logger, rpc)
		req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		first, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		producer := historydlq.TaskIdentity{
			NamespaceID: req.SourceTaskIdentity.NamespaceId,
			BusinessID:  req.SourceTaskIdentity.BusinessId,
			RunID:       primitives.NewUUID().String(),
		}
		secondReq := newHistoryDLQProducerEnqueueRequest(t, env, 1<<40, 1024, producer)
		second, err := rpc.Enqueue(t.Context(), secondReq)
		require.NoError(t, err)
		require.Equal(t, first.Ref.BusinessId, second.Ref.BusinessId)
		require.Equal(t, first.Ref.RunId, second.Ref.RunId)
		require.NotEqual(t, first.Ref.EntryKey, second.Ref.EntryKey)
		env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
		again, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, first, again)
		read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               first.Ref,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &historydlqpb.StoredMessage{
			Ref:                first.Ref,
			SourceTaskIdentity: req.SourceTaskIdentity,
			Task:               req.Task,
		}, read.Message)
		for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
			_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               first.Ref,
				Resolution:        reason,
			})
			require.NoError(t, err)
		}
		env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
		_, err = rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               first.Ref,
		})
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
		again, err = rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, first.Ref.RunId, again.Ref.RunId)
		require.Equal(t, first.Ref.EntryKey, again.Ref.EntryKey)
		appRef := historydlq.MessageRef{
			BusinessID: again.Ref.BusinessId,
			RunID:      again.Ref.RunId,
			EntryKey:   again.Ref.EntryKey,
			MessageID:  again.Ref.MessageId,
			Queue: historydlq.QueueKey{
				CategoryID:    req.Queue.TaskCategory,
				SourceCluster: req.Queue.SourceCluster,
				TargetCluster: req.Queue.TargetCluster,
			},
			EnqueuedAt: again.Ref.EnqueuedAt.AsTime(),
		}
		stored, err := client.ReadMessage(t.Context(), appRef)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, req.Task, stored.Task)
		require.NotNil(t, stored.Task)
		read, err = rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               second.Ref,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, secondReq.Task, read.Message.Task)
		for _, bad := range []string{"run", "entry"} {
			ref := proto.Clone(first.Ref).(*historydlqpb.MessageRef)
			if bad == "run" {
				ref.RunId = primitives.NewUUID().String()
			} else {
				ref.EntryKey = "missing"
			}
			_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Ref:               ref,
				Resolution:        historydlqpb.RESOLUTION_PURGED,
			})
			if bad == "run" {
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			} else {
				require.NoError(t, err)
			}
		}
	})
	t.Run("ResolveByEntryLocator", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		accepted, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               accepted.Ref,
			Resolution:        historydlqpb.RESOLUTION_PURGED,
		})
		require.NoError(t, err)
		recreated, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		otherReq := proto.Clone(req).(*historydlqpb.EnqueueRequest)
		otherReq.SourceTaskIdentity.TaskId++
		other, err := rpc.Enqueue(t.Context(), otherReq)
		require.NoError(t, err)
		locator := proto.Clone(accepted.Ref).(*historydlqpb.MessageRef)
		locator.EnqueuedAt = &timestamppb.Timestamp{Seconds: 1 << 62}
		locator.MessageId = -1
		_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               locator,
			Resolution:        historydlqpb.RESOLUTION_MERGED,
		})
		require.NoError(t, err)
		env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
		_, err = rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: recreated.Ref})
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
		read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: other.Ref})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, otherReq.Task, read.Message.Task)
		locator.EnqueuedAt = nil
		locator.MessageId = 0
		_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               locator,
			Resolution:        historydlqpb.RESOLUTION_PURGED,
		})
		require.NoError(t, err)
	})
	for _, existing := range []bool{false, true} {
		for _, duplicate := range []bool{false, true} {
			name := fmt.Sprintf("Concurrent/existing=%t/duplicate=%t", existing, duplicate)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				method := "CreateWorkflowExecution"
				if existing {
					method = "UpdateWorkflowExecution"
				}
				gate, opt := testcore.NewPersistenceCallGate(testcore.WithStore(config.ExecutionStoreName), testcore.WithMethod(method))
				env := newHistoryDLQTestEnvironment(t, opt)
				rpc := env.GetTestCluster().Host().HistoryDLQClient()
				req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
				if existing {
					seed := proto.Clone(req).(*historydlqpb.EnqueueRequest)
					seed.SourceTaskIdentity.TaskId = 122
					_, err := rpc.Enqueue(t.Context(), seed)
					require.NoError(t, err)
				}
				// Capture system-namespace RPC admission on this dedicated cluster.
				handler, ok := env.GetTestCluster().Host().GetMetricsHandler().(*metricstest.CaptureHandler)
				require.True(t, ok)
				capture := handler.StartCapture()
				t.Cleanup(func() { handler.StopCapture(capture) })
				type result struct {
					response *historydlqpb.EnqueueResponse
					err      error
				}
				results := make(chan result, 2)
				go func() {
					response, err := rpc.Enqueue(t.Context(), req)
					results <- result{response, err}
				}()
				testcore.WaitForPersistenceCall(t, gate, 10*time.Second)
				select {
				case result := <-results:
					t.Fatalf("enqueue returned before persistence: %v", result)
				default:
				}
				other := proto.Clone(req).(*historydlqpb.EnqueueRequest)
				if !duplicate {
					other.SourceTaskIdentity.TaskId += 1 << 40
					other.SourceTaskIdentity.RunId = primitives.NewUUID().String()
				}
				go func() {
					response, err := rpc.Enqueue(t.Context(), other)
					results <- result{response, err}
				}()
				await.Require(
					t.Context(),
					t,
					func(t *await.T) {
						var received int
						for _, recording := range capture.Snapshot()[metrics.ServiceRequests.Name()] {
							if recording.Tags["operation"] == "Enqueue" {
								received++
							}
						}
						require.Equal(t, 2, received)
					},
					10*time.Second,
					time.Millisecond,
				)
				gate.Release()
				first, second := await.Rcv(t, results), await.Rcv(t, results)
				require.NoError(t, first.err)
				require.NoError(t, second.err)
				require.Equal(t, first.response.Ref.BusinessId, second.response.Ref.BusinessId)
				require.Equal(t, first.response.Ref.RunId, second.response.Ref.RunId)
				if duplicate {
					protorequire.ProtoEqual(t, first.response, second.response)
				} else {
					require.NotEqual(t, first.response.Ref.EntryKey, second.response.Ref.EntryKey)
				}
				env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
				for _, ref := range []*historydlqpb.MessageRef{first.response.Ref, second.response.Ref} {
					read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
						SystemNamespaceId: primitives.SystemNamespaceID,
						Ref:               ref,
					})
					require.NoError(t, err)
					protorequire.ProtoEqual(t, req.Task, read.Message.Task)
				}
			})
		}
		t.Run(fmt.Sprintf("LostResponse/existing=%t", existing), func(t *testing.T) {
			t.Parallel()
			env := newHistoryDLQTestEnvironment(t)
			rpc := env.GetTestCluster().Host().HistoryDLQClient()
			req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
			if existing {
				seed := proto.Clone(req).(*historydlqpb.EnqueueRequest)
				seed.SourceTaskIdentity.TaskId = 122
				_, err := rpc.Enqueue(t.Context(), seed)
				require.NoError(t, err)
			}
			lost := &lostDLQResponseClient{HistoryDLQServiceClient: rpc}
			_, err := lost.Enqueue(t.Context(), req)
			require.Error(t, err)
			require.NotNil(t, lost.accepted)
			env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
			retry, err := rpc.Enqueue(t.Context(), req)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, lost.accepted, retry)
		})
		t.Run(fmt.Sprintf("PersistenceFailure/existing=%t", existing), func(t *testing.T) {
			t.Parallel()
			var fail atomic.Bool
			method := "CreateWorkflowExecution"
			if existing {
				method = "UpdateWorkflowExecution"
			}
			env := newHistoryDLQTestEnvironment(
				t,
				testcore.WithDisableTestloggerFailure(),
				testcore.InjectPersistenceFault(
					func(config.FaultInjectionTarget) error {
						if fail.Load() {
							return serviceerror.NewUnavailable("DLQ persistence failed")
						}
						return nil
					},
					testcore.WithStore(config.ExecutionStoreName),
					testcore.WithMethod(method),
				),
			)
			rpc := env.GetTestCluster().Host().HistoryDLQClient()
			req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
			if existing {
				seed := proto.Clone(req).(*historydlqpb.EnqueueRequest)
				seed.SourceTaskIdentity.TaskId = 122
				_, err := rpc.Enqueue(t.Context(), seed)
				require.NoError(t, err)
			}
			fail.Store(true)
			response, err := rpc.Enqueue(t.Context(), req)
			require.Error(t, err)
			require.Nil(t, response)
			fail.Store(false)
			env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
			response, err = rpc.Enqueue(t.Context(), req)
			require.NoError(t, err)
			require.NotEmpty(t, response.Ref.RunId)
		})
	}
	t.Run("ResolveEntriesPersistenceFailure", func(t *testing.T) {
		t.Parallel()
		var fail atomic.Bool
		env := newHistoryDLQTestEnvironment(t,
			testcore.WithDisableTestloggerFailure(),
			testcore.InjectPersistenceFault(func(config.FaultInjectionTarget) error {
				if fail.Load() {
					return serviceerror.NewUnavailable("batch resolve persistence failed")
				}
				return nil
			}, testcore.WithStore(config.ExecutionStoreName), testcore.WithMethod("UpdateWorkflowExecution")),
		)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		request := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		var refs []*historydlqpb.MessageRef
		for _, id := range []int64{123, 124, 125} {
			request.SourceTaskIdentity.TaskId = id
			accepted, err := rpc.Enqueue(t.Context(), request)
			require.NoError(t, err)
			refs = append(refs, accepted.Ref)
		}
		registry := chasm.NewRegistry(env.Logger)
		require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
		require.NoError(t, registry.Register(historydlq.NewNilLibrary()))
		archetypeID, ok := registry.ArchetypeIDOf(reflect.TypeFor[*historydlq.Segment]())
		require.True(t, ok)
		readState := func() *historydlqpb.SegmentState {
			_, err := rpc.DescribeSegment(t.Context(), &historydlqpb.DescribeSegmentRequest{
				SystemNamespaceId: primitives.SystemNamespaceID,
				Segment:           &historydlqpb.SegmentRef{Queue: request.Queue, BusinessId: request.BusinessId, RunId: refs[0].RunId},
			})
			require.NoError(t, err)
			state, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(t.Context(), &persistence.GetWorkflowExecutionRequest{
				ShardID:     common.WorkflowIDToHistoryShard(primitives.SystemNamespaceID, request.BusinessId, 4),
				NamespaceID: primitives.SystemNamespaceID,
				WorkflowID:  request.BusinessId,
				RunID:       refs[0].RunId,
				ArchetypeID: archetypeID,
			})
			require.NoError(t, err)
			segment, _, err := chasm.NewDetachedExecution[*historydlq.Segment](t.Context(), state.State, registry)
			require.NoError(t, err)
			return segment.SegmentState
		}
		awaitMinimum := func(minimum int64) {
			await.Require(t.Context(), t, func(t *await.T) {
				ctx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
				require.NoError(t, err)
				response, err := chasm.ListExecutions[*historydlq.Segment, *emptypb.Empty](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: primitives.SystemLocalNamespace,
					Query:         fmt.Sprintf("HistoryDLQSourceCluster = '%s' AND HistoryDLQMinMessageID = %d", request.Queue.SourceCluster, minimum),
					PageSize:      10,
				})
				require.NoError(t, err)
				require.Len(t, response.Executions, 1)
				require.Equal(t, refs[0].RunId, response.Executions[0].RunID)
				require.Equal(t, request.BusinessId, response.Executions[0].BusinessID)
			}, 10*time.Second, 100*time.Millisecond)
		}
		before := readState()
		awaitMinimum(before.MinMessageId)
		// Keep one entry with the highest numeric ID to verify the remaining minimum.
		slices.SortFunc(refs, func(a, b *historydlqpb.MessageRef) int { return cmp.Compare(a.MessageId, b.MessageId) })
		resolve := &historydlqpb.ResolveEntriesRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Segment:           &historydlqpb.SegmentRef{Queue: request.Queue, BusinessId: request.BusinessId, RunId: refs[0].RunId},
			EntryKeys:         []string{refs[0].EntryKey, refs[1].EntryKey, refs[0].EntryKey},
			Resolution:        historydlqpb.RESOLUTION_PURGED,
		}
		fail.Store(true)
		response, err := rpc.ResolveEntries(t.Context(), resolve)
		require.Error(t, err)
		require.Nil(t, response)
		fail.Store(false)
		env.CloseShard(primitives.SystemNamespaceID, request.BusinessId)
		protorequire.ProtoEqual(t, before, readState())
		awaitMinimum(before.MinMessageId)
		for _, ref := range refs {
			message, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: ref})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, request.Task, message.Message.Task)
		}
		_, err = rpc.ResolveEntries(t.Context(), resolve)
		require.NoError(t, err)
		env.CloseShard(primitives.SystemNamespaceID, request.BusinessId)
		after := readState()
		require.Equal(t, refs[2].MessageId, after.MinMessageId)
		awaitMinimum(after.MinMessageId)
		require.Less(t, after.RetainedBytes, before.RetainedBytes)
		_, err = rpc.ResolveEntries(t.Context(), resolve)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, after, readState())
		for _, ref := range refs[:2] {
			_, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: ref})
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
		}
		resolve.EntryKeys = []string{refs[2].EntryKey}
		_, err = rpc.ResolveEntries(t.Context(), resolve)
		require.NoError(t, err)
		env.CloseShard(primitives.SystemNamespaceID, request.BusinessId)
		empty := readState()
		require.Equal(t, int64(-1), empty.MinMessageId)
		require.Less(t, empty.RetainedBytes, after.RetainedBytes)
		awaitMinimum(-1)
	})
	t.Run("ResolvePersistenceFailure", func(t *testing.T) {
		t.Parallel()
		var fail atomic.Bool
		env := newHistoryDLQTestEnvironment(
			t,
			testcore.WithDisableTestloggerFailure(),
			testcore.InjectPersistenceFault(
				func(config.FaultInjectionTarget) error {
					if fail.Load() {
						return serviceerror.NewUnavailable("resolve persistence failed")
					}
					return nil
				},
				testcore.WithStore(config.ExecutionStoreName),
				testcore.WithMethod("UpdateWorkflowExecution"),
			),
		)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		req := newHistoryDLQEnqueueRequest(t, env, 123, 1024)
		accepted, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		resolve := &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               accepted.Ref,
			Resolution:        historydlqpb.RESOLUTION_PURGED,
		}
		fail.Store(true)
		response, err := rpc.Resolve(t.Context(), resolve)
		require.Error(t, err)
		require.Nil(t, response)
		fail.Store(false)
		env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
		read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               accepted.Ref,
		})
		require.NoError(t, err)
		require.NotNil(t, read.Message.Task)
		protorequire.ProtoEqual(t, req.Task, read.Message.Task)
		_, err = rpc.Resolve(t.Context(), resolve)
		require.NoError(t, err)
	})
	t.Run("SegmentBackpressure", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t)
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		req := newHistoryDLQEnqueueRequest(t, env, 123, 3<<20)
		first, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		other := newHistoryDLQEnqueueRequest(t, env, 124, 2<<20)
		response, err := rpc.Enqueue(t.Context(), other)
		require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
		require.Nil(t, response)
		again, err := rpc.Enqueue(t.Context(), req)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, first, again)
		env.CloseShard(primitives.SystemNamespaceID, req.BusinessId)
		_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               first.Ref,
			Resolution:        historydlqpb.RESOLUTION_PURGED,
		})
		require.NoError(t, err)
		response, err = rpc.Enqueue(t.Context(), other)
		require.NoError(t, err)
		require.Equal(t, first.Ref.RunId, response.Ref.RunId)
	})
	t.Run("NumericCollision", func(t *testing.T) {
		t.Parallel()
		env := newHistoryDLQTestEnvironment(t, testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQReadCHASM, true))
		rpc := env.GetTestCluster().Host().HistoryDLQClient()
		engineCtx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
		require.NoError(t, err)
		var refs []*historydlqpb.MessageRef
		var queue *commonspb.HistoryDLQKey
		var want []*commonspb.HistoryDLQTask
		for _, id := range []int64{123, 124} {
			req := newHistoryDLQEnqueueRequest(t, env, id, 1024)
			input := historydlq.EnqueueRequest{
				Queue: historydlq.QueueKey{
					CategoryID:    req.Queue.TaskCategory,
					SourceCluster: req.Queue.SourceCluster,
					TargetCluster: req.Queue.TargetCluster,
				},
				SourceTaskIdentity: historydlq.TaskIdentity{
					SourceShardID: 7,
					TaskID:        id,
					NamespaceID:   req.SourceTaskIdentity.NamespaceId,
					BusinessID:    req.SourceTaskIdentity.BusinessId,
					RunID:         req.SourceTaskIdentity.RunId,
					ArchetypeID:   req.SourceTaskIdentity.ArchetypeId,
				},
				Task: req.Task,
			}
			_, err := chasm.UpdateWithStartExecution[*historydlq.Segment](
				engineCtx,
				chasm.ExecutionKey{
					NamespaceID: primitives.SystemNamespaceID,
					BusinessID:  req.BusinessId,
				},
				historydlq.NewSegment,
				func(
					s *historydlq.Segment,
					ctx chasm.MutableContext,
					input historydlq.EnqueueRequest,
				) (struct{}, error) {
					ref, err := s.Append(ctx, input)
					if err != nil {
						return struct{}{}, err
					}
					entry := s.Entries[ref.MessageRef.EntryKey].Get(ctx)
					before := proto.Size(entry.EntryState)
					entry.MessageId = 0
					s.RetainedBytes += int64(proto.Size(entry.EntryState) - before)
					s.MinMessageId = 0
					return struct{}{}, nil
				},
				input,
			)
			require.NoError(t, err)
			response, err := rpc.Enqueue(t.Context(), req)
			require.NoError(t, err)
			refs = append(refs, response.Ref)
			queue = req.Queue
			want = append(want, &commonspb.HistoryDLQTask{
				Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: 0}, Payload: req.Task,
			})
		}
		require.Equal(t, refs[0].RunId, refs[1].RunId)
		require.Equal(t, refs[0].MessageId, refs[1].MessageId)
		require.NotEqual(t, refs[0].EntryKey, refs[1].EntryKey)
		manager, err := env.GetTestCluster().TestBase().Factory.NewHistoryTaskQueueManager()
		require.NoError(t, err)
		t.Cleanup(manager.Close)
		legacyKey := persistence.QueueKey{
			QueueType: persistence.QueueTypeHistoryDLQ, Category: tasks.CategoryReplication,
			SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster,
		}
		_, err = manager.CreateQueue(t.Context(), &persistence.CreateQueueRequest{QueueKey: legacyKey})
		require.NoError(t, err)
		task, err := serialization.NewSerializer().DeserializeTask(tasks.CategoryReplication, want[0].Payload.Blob)
		require.NoError(t, err)
		_, err = manager.EnqueueTask(t.Context(), &persistence.EnqueueTaskRequest{
			QueueType: persistence.QueueTypeHistoryDLQ, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster,
			SourceShardID: 9, Task: task,
		})
		require.NoError(t, err)
		want = append(want, &commonspb.HistoryDLQTask{
			Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: 0},
			Payload:  &commonspb.HistoryTask{ShardId: 9, Blob: want[0].Payload.Blob},
		})
		bound := &commonspb.HistoryDLQTaskMetadata{MessageId: 0}
		await.Require(t.Context(), t, func(t *await.T) {
			page, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{
				DlqKey: queue, PageSize: 10, InclusiveMaxTaskMetadata: bound,
			})
			require.NoError(t, err)
			protorequire.ProtoElementsMatch(t, want, page.DlqTasks)
		}, 15*time.Second, 50*time.Millisecond)
		var got []*commonspb.HistoryDLQTask
		var token []byte
		for i := 0; ; i++ {
			require.Less(t, i, 4)
			page, err := env.AdminClient().GetDLQTasks(t.Context(), &adminservice.GetDLQTasksRequest{
				DlqKey: queue, PageSize: 1, InclusiveMaxTaskMetadata: bound, NextPageToken: token,
			})
			require.NoError(t, err)
			require.Len(t, page.DlqTasks, 1)
			got = append(got, page.DlqTasks...)
			token = page.NextPageToken
			if len(token) == 0 {
				break
			}
		}
		protorequire.ProtoElementsMatch(t, want, got)
		_, err = rpc.Resolve(t.Context(), &historydlqpb.ResolveRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               refs[0],
			Resolution:        historydlqpb.RESOLUTION_PURGED,
		})
		require.NoError(t, err)
		read, err := rpc.ReadMessage(t.Context(), &historydlqpb.ReadMessageRequest{
			SystemNamespaceId: primitives.SystemNamespaceID,
			Ref:               refs[1],
		})
		require.NoError(t, err)
		require.NotNil(t, read.Message.Task)
	})
}
