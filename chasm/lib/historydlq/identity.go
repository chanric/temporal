package historydlq

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
)

type QueueKey struct {
	CategoryID    int32
	SourceCluster string
	TargetCluster string
}

// TaskIdentity refers to the original persisted task, before DLQ placement.
// Execution fields are metadata and do not participate in deduplication.
type TaskIdentity struct {
	SourceShardID int32
	TaskID        int64
	NamespaceID   string
	BusinessID    string
	RunID         string
	ArchetypeID   uint32
}

type EnqueueRequest struct {
	Queue    QueueKey
	Original TaskIdentity
	Task     *commonspb.HistoryTask
}

type MessageRef struct {
	NamespaceID string
	BusinessID  string
	RunID       string
	MessageID   int64
	Queue       QueueKey
	EnqueuedAt  time.Time
}

func identityHash(queue QueueKey, original TaskIdentity) ([sha256.Size]byte, error) {
	if queue.CategoryID <= 0 || queue.SourceCluster == "" || queue.TargetCluster == "" ||
		original.SourceShardID <= 0 || original.TaskID <= 0 {
		return [sha256.Size]byte{}, serviceerror.NewInvalidArgument("history DLQ requires a queue and original persisted task identity")
	}
	// Quoted strings preserve tuple boundaries, including delimiters and escapes.
	tuple := fmt.Sprintf("historydlq:v1:%d:%q:%q:%d:%d",
		queue.CategoryID, queue.SourceCluster, queue.TargetCluster, original.SourceShardID, original.TaskID)
	return sha256.Sum256([]byte(tuple)), nil
}

func BusinessID(queue QueueKey, original TaskIdentity) (string, error) {
	hash, err := identityHash(queue, original)
	if err != nil {
		return "", err
	}
	return "historydlq:v1:" + hex.EncodeToString(hash[:]), nil
}

func messageID(queue QueueKey, original TaskIdentity, enqueuedAt time.Time) (int64, error) {
	// Versions 1–3 are reserved for future numeric formats.
	const version int64 = 0

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if enqueuedAt.Before(epoch) || !enqueuedAt.Before(epoch.Add(time.Duration(1<<41)*time.Millisecond)) {
		return 0, serviceerror.NewInvalidArgument("history DLQ enqueue time is outside the message ID range")
	}
	hash, err := identityHash(queue, original)
	if err != nil {
		return 0, err
	}
	elapsedMilliseconds := enqueuedAt.UnixMilli() - epoch.UnixMilli()
	hash20 := int64(binary.BigEndian.Uint32(hash[28:]) & ((1 << 20) - 1))
	return (version << 61) | (elapsedMilliseconds << 20) | hash20, nil
}
