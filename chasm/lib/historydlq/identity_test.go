package historydlq

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testRequest() EnqueueRequest {
	return EnqueueRequest{
		Queue:              QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"},
		SourceTaskIdentity: TaskIdentity{SourceShardID: 7, TaskID: 123, NamespaceID: "namespace", BusinessID: "workflow", RunID: "run", ArchetypeID: 1},
	}
}

func TestEntryKeyStableIdentity(t *testing.T) {
	t.Parallel()
	req := testRequest()
	id, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	retryID, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, id, retryID)
	changes := map[string]func(*EnqueueRequest){
		"category":         func(r *EnqueueRequest) { r.Queue.CategoryID++ },
		"source":           func(r *EnqueueRequest) { r.Queue.SourceCluster += "x" },
		"target":           func(r *EnqueueRequest) { r.Queue.TargetCluster += "x" },
		"source shard":     func(r *EnqueueRequest) { r.SourceTaskIdentity.SourceShardID++ },
		"task":             func(r *EnqueueRequest) { r.SourceTaskIdentity.TaskID++ },
		"tuple boundaries": func(r *EnqueueRequest) { r.Queue.SourceCluster = "sour"; r.Queue.TargetCluster = "cetarget" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := req
			change(&changed)
			other, err := EntryKey(changed.Queue, changed.SourceTaskIdentity)
			require.NoError(t, err)
			require.NotEqual(t, id, other)
		})
	}
}

func TestSegmentBusinessIDAndEntryKey(t *testing.T) {
	t.Parallel()
	req := testRequest()
	segmentID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, "historydlq:segment:v0:bc6630da1fbb56f1511981a83767fd8a42e5b20bbffa2b2e84b79d57c4389de8", segmentID)
	entryKey, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, "4a76d13b0b77cdd05d1b1197dcd6dcdd560b5984e9e1b047635b1e146578e1bb", entryKey)

	changed := req
	changed.SourceTaskIdentity.TaskID++
	unchangedSegmentID, err := SegmentBusinessID(changed.Queue, changed.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, segmentID, unchangedSegmentID)
	otherEntryKey, err := EntryKey(changed.Queue, changed.SourceTaskIdentity)
	require.NoError(t, err)
	require.NotEqual(t, entryKey, otherEntryKey)
}

func TestSegmentBusinessIDTaskIDsDoNotChangePool(t *testing.T) {
	t.Parallel()
	req := testRequest()
	for _, tc := range []struct {
		name     string
		firstID  int64
		secondID int64
	}{
		{"first and distant", 1, 128},
		{"distant IDs", 128, 1 << 40},
		{"last IDs", math.MaxInt64 - 126, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := req.SourceTaskIdentity, req.SourceTaskIdentity
			first.TaskID, second.TaskID = tc.firstID, tc.secondID
			firstID, err := SegmentBusinessID(req.Queue, first)
			require.NoError(t, err)
			secondID, err := SegmentBusinessID(req.Queue, second)
			require.NoError(t, err)
			require.Equal(t, firstID, secondID)
			second.SourceShardID++
			otherShardID, err := SegmentBusinessID(req.Queue, second)
			require.NoError(t, err)
			require.NotEqual(t, firstID, otherShardID)
		})
	}
}

func TestSegmentBucketID(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		identity TaskIdentity
		want     uint32
	}{
		{"bucket zero", TaskIdentity{NamespaceID: "namespace-9", ArchetypeID: 9, BusinessID: "business-9"}, 0},
		{"bucket one", TaskIdentity{NamespaceID: "namespace-0", ArchetypeID: 0, BusinessID: "business-0"}, 1},
		{"bucket two", TaskIdentity{NamespaceID: "namespace-2", ArchetypeID: 2, BusinessID: "business-2"}, 2},
		{"bucket three", TaskIdentity{NamespaceID: "namespace-1", ArchetypeID: 1, BusinessID: "business-1"}, 3},
		{"empty metadata", TaskIdentity{}, 0},
		{"namespace participates", TaskIdentity{NamespaceID: "namespace-other", ArchetypeID: 1, BusinessID: "workflow"}, 0},
		{"archetype participates", TaskIdentity{NamespaceID: "namespace", ArchetypeID: 2, BusinessID: "workflow"}, 2},
		{"business ID participates", TaskIdentity{NamespaceID: "namespace", ArchetypeID: 1, BusinessID: "workflow-other"}, 2},
		{"delimiter in namespace", TaskIdentity{NamespaceID: "a:b", ArchetypeID: 1, BusinessID: "c"}, 3},
		{"delimiter in business ID", TaskIdentity{NamespaceID: "a", ArchetypeID: 1, BusinessID: "b:c"}, 1},
		{"quotes and escapes", TaskIdentity{NamespaceID: "namespace:%q", ArchetypeID: 1, BusinessID: "business:\n"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SegmentBucketID(tc.identity)
			require.Equal(t, tc.want, got)
			require.Less(t, got, uint32(4))
		})
	}
}

func TestSegmentBusinessIDBucketPools(t *testing.T) {
	t.Parallel()
	req := testRequest()
	for _, tc := range []struct {
		name       string
		producer   TaskIdentity
		businessID string
	}{
		{"bucket zero", TaskIdentity{NamespaceID: "namespace-9", ArchetypeID: 9, BusinessID: "business-9"}, "historydlq:segment:v0:025659d27d3d750126345cace8e9c7622c5f587861db3bafca082bd67a547ed2"},
		{"bucket one", TaskIdentity{NamespaceID: "namespace-0", ArchetypeID: 0, BusinessID: "business-0"}, "historydlq:segment:v0:bc6630da1fbb56f1511981a83767fd8a42e5b20bbffa2b2e84b79d57c4389de8"},
		{"bucket two", TaskIdentity{NamespaceID: "namespace-2", ArchetypeID: 2, BusinessID: "business-2"}, "historydlq:segment:v0:33291c30bf9e23b6999d928f041687694b44a6269abf3a34714194f95e8d7930"},
		{"bucket three", TaskIdentity{NamespaceID: "namespace-1", ArchetypeID: 1, BusinessID: "business-1"}, "historydlq:segment:v0:0ba2e58fcb55415b70f4aae02427c3c2a5645eec058ea08a0b58cabfd52288ab"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			producer := req.SourceTaskIdentity
			producer.NamespaceID = tc.producer.NamespaceID
			producer.ArchetypeID = tc.producer.ArchetypeID
			producer.BusinessID = tc.producer.BusinessID
			got, err := SegmentBusinessID(req.Queue, producer)
			require.NoError(t, err)
			require.Equal(t, tc.businessID, got)
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*TaskIdentity)
	}{
		{"task ID", func(identity *TaskIdentity) { identity.TaskID += 1 << 40 }},
		{"run ID", func(identity *TaskIdentity) { identity.RunID = "another-run" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := req.SourceTaskIdentity
			tc.mutate(&changed)
			got, err := SegmentBusinessID(req.Queue, changed)
			require.NoError(t, err)
			want, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestSegmentProducerRouting(t *testing.T) {
	t.Parallel()
	req := testRequest()
	require.Equal(t, uint32(1), SegmentBucketID(req.SourceTaskIdentity))
	baseID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		mutate     func(*EnqueueRequest)
		wantBucket uint32
	}{
		{"namespace", func(r *EnqueueRequest) { r.SourceTaskIdentity.NamespaceID += "-other" }, 0},
		{"archetype", func(r *EnqueueRequest) { r.SourceTaskIdentity.ArchetypeID++ }, 2},
		{"business", func(r *EnqueueRequest) { r.SourceTaskIdentity.BusinessID += "-other" }, 2},
		{"category", func(r *EnqueueRequest) { r.Queue.CategoryID++ }, 1},
		{"source cluster", func(r *EnqueueRequest) { r.Queue.SourceCluster += "-other" }, 1},
		{"target cluster", func(r *EnqueueRequest) { r.Queue.TargetCluster += "-other" }, 1},
		{"source shard", func(r *EnqueueRequest) { r.SourceTaskIdentity.SourceShardID++ }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := req
			tc.mutate(&changed)
			require.Equal(t, tc.wantBucket, SegmentBucketID(changed.SourceTaskIdentity))
			changedID, err := SegmentBusinessID(changed.Queue, changed.SourceTaskIdentity)
			require.NoError(t, err)
			require.NotEqual(t, baseID, changedID)
		})
	}
}

func TestMessageID(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := testRequest()
	for _, tc := range []struct {
		name             string
		offset           time.Duration
		wantMilliseconds int64
	}{
		{"epoch", 0, 0},
		{"next millisecond", time.Millisecond, 1},
		{"sub-millisecond precision", 1234*time.Millisecond + 999999*time.Nanosecond, 1234},
		{"last timestamp", time.Duration((1<<41)-1) * time.Millisecond, (1 << 41) - 1},
		{"last nanosecond", time.Duration(1<<41)*time.Millisecond - time.Nanosecond, (1 << 41) - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id, err := messageID(req.Queue, req.SourceTaskIdentity, epoch.Add(tc.offset))
			require.NoError(t, err)
			// The fixture's SHA-256 suffix is 0x6578e1bb; its low 20 bits are 582075.
			require.Equal(t, (tc.wantMilliseconds<<20)|int64(582075), id)
			require.GreaterOrEqual(t, id, int64(0))
			require.Less(t, id, int64(1<<61))
			require.Equal(t, tc.wantMilliseconds, (id>>20)&((1<<41)-1))
			require.Equal(t, int64(582075), id&((1<<20)-1))
		})
	}
	for _, tc := range []struct {
		name       string
		enqueuedAt time.Time
	}{
		{"before epoch", epoch.Add(-time.Nanosecond)},
		{"timestamp overflow", epoch.Add(time.Duration(1<<41) * time.Millisecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id, err := messageID(req.Queue, req.SourceTaskIdentity, tc.enqueuedAt)
			require.Error(t, err)
			require.Zero(t, id)
		})
	}
	businessID, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, "4a76d13b0b77cdd05d1b1197dcd6dcdd560b5984e9e1b047635b1e146578e1bb", businessID)
}

func TestIdentityRequiresQueueAndPersistedTask(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*EnqueueRequest){
		"category":     func(req *EnqueueRequest) { req.Queue.CategoryID = 0 },
		"source":       func(req *EnqueueRequest) { req.Queue.SourceCluster = "" },
		"target":       func(req *EnqueueRequest) { req.Queue.TargetCluster = "" },
		"source shard": func(req *EnqueueRequest) { req.SourceTaskIdentity.SourceShardID = 0 },
		"task":         func(req *EnqueueRequest) { req.SourceTaskIdentity.TaskID = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req := testRequest()
			change(&req)
			id, err := EntryKey(req.Queue, req.SourceTaskIdentity)
			require.Error(t, err)
			require.Empty(t, id)
			id, err = SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
			require.Error(t, err)
			require.Empty(t, id)
		})
	}
}
