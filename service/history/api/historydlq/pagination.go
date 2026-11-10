package historydlq

import (
	"bytes"
	"cmp"
	"encoding/json"
	"io"

	"go.temporal.io/api/serviceerror"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
)

const readTokenPrefix = "historydlq:read:"

type readTuple struct {
	MessageID                   int64
	Backend                     int
	BusinessID, RunID, EntryKey string
}

type readCursor struct {
	Version int
	Bound   int64
	Queue   dlq.QueueKey
	Mode    string
	Last    readTuple
}

func compareTuple(a, b readTuple) int {
	for _, order := range []int{cmp.Compare(a.MessageID, b.MessageID), cmp.Compare(a.Backend, b.Backend), cmp.Compare(a.BusinessID, b.BusinessID), cmp.Compare(a.RunID, b.RunID), cmp.Compare(a.EntryKey, b.EntryKey)} {
		if order != 0 {
			return order
		}
	}
	return 0
}

func encodeReadCursor(queue dlq.QueueKey, bound int64, last readTuple) []byte {
	body, _ := json.Marshal(readCursor{Version: 1, Bound: bound, Queue: queue, Mode: "mixed", Last: last})
	return append([]byte(readTokenPrefix), body...)
}

// decodeReadCursor decodes mixed-store tokens. Nonempty tokens without the mixed-store
// prefix are treated as QueueV2 continuations and must be forwarded unchanged.
// An empty token starts a fresh read.
func decodeReadCursor(
	token []byte,
	queue dlq.QueueKey,
	bound int64,
	includeCHASM bool,
) (lastReadTuple *readTuple, isQueueV2Token bool, err error) {
	if len(token) == 0 {
		return nil, false, nil
	}
	if !bytes.HasPrefix(token, []byte(readTokenPrefix)) {
		return nil, true, nil
	}
	invalid := func() (*readTuple, bool, error) {
		return nil, false, serviceerror.NewInvalidArgument("history DLQ page token does not match the request")
	}
	var cursor struct {
		Version int
		Bound   *int64
		Queue   dlq.QueueKey
		Mode    string
		Last    *struct {
			MessageID                   *int64
			Backend                     *int
			BusinessID, RunID, EntryKey string
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(token[len(readTokenPrefix):]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cursor) != nil || decoder.Decode(new(any)) != io.EOF || cursor.Version != 1 || cursor.Bound == nil || *cursor.Bound != bound || cursor.Queue != queue || cursor.Mode != "mixed" || !includeCHASM || cursor.Last == nil || cursor.Last.MessageID == nil || cursor.Last.Backend == nil {
		return invalid()
	}
	last := readTuple{MessageID: *cursor.Last.MessageID, Backend: *cursor.Last.Backend, BusinessID: cursor.Last.BusinessID, RunID: cursor.Last.RunID, EntryKey: cursor.Last.EntryKey}
	if last.MessageID < 0 || last.MessageID > bound {
		return invalid()
	}
	switch last.Backend {
	case readBackendQueueV2:
		if last.BusinessID != "" || last.RunID != "" || last.EntryKey != "" {
			return invalid()
		}
	case readBackendCHASM:
		if last.BusinessID == "" || last.RunID == "" || last.EntryKey == "" {
			return invalid()
		}
	default:
		return invalid()
	}
	return &last, false, nil
}
