package historydlq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testRequest() EnqueueRequest {
	return EnqueueRequest{
		Queue:    QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"},
		Original: TaskIdentity{SourceShardID: 7, TaskID: 123, NamespaceID: "namespace", BusinessID: "workflow", RunID: "run", ArchetypeID: 1},
	}
}

func TestBusinessID(t *testing.T) {
	t.Parallel()
	req := testRequest()
	id, err := BusinessID(req.Queue, req.Original)
	require.NoError(t, err)
	retryID, err := BusinessID(req.Queue, req.Original)
	require.NoError(t, err)
	require.Equal(t, id, retryID)
	changes := map[string]func(*EnqueueRequest){
		"category":         func(r *EnqueueRequest) { r.Queue.CategoryID++ },
		"source":           func(r *EnqueueRequest) { r.Queue.SourceCluster += "x" },
		"target":           func(r *EnqueueRequest) { r.Queue.TargetCluster += "x" },
		"source shard":     func(r *EnqueueRequest) { r.Original.SourceShardID++ },
		"task":             func(r *EnqueueRequest) { r.Original.TaskID++ },
		"tuple boundaries": func(r *EnqueueRequest) { r.Queue.SourceCluster = "sour"; r.Queue.TargetCluster = "cetarget" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := req
			change(&changed)
			other, err := BusinessID(changed.Queue, changed.Original)
			require.NoError(t, err)
			require.NotEqual(t, id, other)
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
			id, err := messageID(req.Queue, req.Original, epoch.Add(tc.offset))
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
			id, err := messageID(req.Queue, req.Original, tc.enqueuedAt)
			require.Error(t, err)
			require.Zero(t, id)
		})
	}
	businessID, err := BusinessID(req.Queue, req.Original)
	require.NoError(t, err)
	require.Equal(t, "historydlq:v1:4a76d13b0b77cdd05d1b1197dcd6dcdd560b5984e9e1b047635b1e146578e1bb", businessID)
}

func TestIdentityRequiresQueueAndPersistedTask(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*EnqueueRequest){
		"category":     func(req *EnqueueRequest) { req.Queue.CategoryID = 0 },
		"source":       func(req *EnqueueRequest) { req.Queue.SourceCluster = "" },
		"target":       func(req *EnqueueRequest) { req.Queue.TargetCluster = "" },
		"source shard": func(req *EnqueueRequest) { req.Original.SourceShardID = 0 },
		"task":         func(req *EnqueueRequest) { req.Original.TaskID = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req := testRequest()
			change(&req)
			id, err := BusinessID(req.Queue, req.Original)
			require.Error(t, err)
			require.Empty(t, id)
		})
	}
}
