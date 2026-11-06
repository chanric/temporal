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

func TestBusinessID(t *testing.T) {
	t.Parallel()
	req := testRequest()
	id, err := BusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	retryID, err := BusinessID(req.Queue, req.SourceTaskIdentity)
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
			other, err := BusinessID(changed.Queue, changed.SourceTaskIdentity)
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
	require.Equal(t, "historydlq:segment:v1:b9ba99e52017e06f6c32221000e633da99cbacbed3cac7764485508f0ff2e885", segmentID)
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

func TestSegmentBusinessIDTaskRanges(t *testing.T) {
	t.Parallel()
	req := testRequest()
	for _, tc := range []struct {
		name       string
		firstID    int64
		secondID   int64
		wantSameID bool
	}{
		{"first range", 1, 128, true},
		{"next range", 128, 129, false},
		{"second range", 129, 256, true},
		{"last range", math.MaxInt64 - 126, math.MaxInt64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := req.SourceTaskIdentity, req.SourceTaskIdentity
			first.TaskID, second.TaskID = tc.firstID, tc.secondID
			firstID, err := SegmentBusinessID(req.Queue, first)
			require.NoError(t, err)
			secondID, err := SegmentBusinessID(req.Queue, second)
			require.NoError(t, err)
			require.Equal(t, tc.wantSameID, firstID == secondID)
			second.SourceShardID++
			otherShardID, err := SegmentBusinessID(req.Queue, second)
			require.NoError(t, err)
			require.NotEqual(t, firstID, otherShardID)
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
			require.Zero(t, id>>61, "versions 1–3 are reserved")
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
	businessID, err := BusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, "historydlq:v1:4a76d13b0b77cdd05d1b1197dcd6dcdd560b5984e9e1b047635b1e146578e1bb", businessID)
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
			id, err := BusinessID(req.Queue, req.SourceTaskIdentity)
			require.Error(t, err)
			require.Empty(t, id)
			id, err = SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
			require.Error(t, err)
			require.Empty(t, id)
		})
	}
}
