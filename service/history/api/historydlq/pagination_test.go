package historydlq

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
)

func TestReadCursorRejectsInvalidEnvelopes(t *testing.T) {
	t.Parallel()
	queue := dlq.QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"}
	for name, change := range map[string]func(*readCursor){
		"version":                func(c *readCursor) { c.Version = 2 },
		"mode":                   func(c *readCursor) { c.Mode = "legacy" },
		"backend":                func(c *readCursor) { c.Last.Backend = 2 },
		"negative id":            func(c *readCursor) { c.Last.MessageID = -1 },
		"legacy identity":        func(c *readCursor) { c.Last.EntryKey = "unexpected" },
		"missing chasm identity": func(c *readCursor) { c.Last.Backend = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cursor := readCursor{Version: 1, Bound: math.MaxInt64, Queue: queue, Mode: "mixed", Last: readTuple{MessageID: 10}}
			change(&cursor)
			body, err := json.Marshal(cursor)
			require.NoError(t, err)
			_, _, err = decodeReadCursor(append([]byte(readTokenPrefix), body...), queue, math.MaxInt64, true)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestReadTupleOrdering(t *testing.T) {
	t.Parallel()
	ordered := []readTuple{
		{MessageID: 9, Backend: 1, BusinessID: "z", RunID: "z", EntryKey: "z"},
		{MessageID: 10},
		{MessageID: 10, Backend: 1, BusinessID: "a", RunID: "a", EntryKey: "a"},
		{MessageID: 10, Backend: 1, BusinessID: "a", RunID: "a", EntryKey: "b"},
		{MessageID: 10, Backend: 1, BusinessID: "a", RunID: "b", EntryKey: "a"},
		{MessageID: 10, Backend: 1, BusinessID: "b", RunID: "a", EntryKey: "a"},
	}
	for i, a := range ordered {
		for j, b := range ordered {
			order := compareTuple(a, b)
			switch {
			case i < j:
				require.Negative(t, order)
			case i > j:
				require.Positive(t, order)
			default:
				require.Zero(t, order)
			}
		}
	}
}

func TestReadCursorRequiresLastTuple(t *testing.T) {
	t.Parallel()
	queue := dlq.QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"}
	for _, last := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`{}`), json.RawMessage(`{"Backend":0}`), json.RawMessage(`{"MessageID":0}`)} {
		t.Run(string(last), func(t *testing.T) {
			t.Parallel()
			envelope := map[string]any{"Version": 1, "Queue": queue, "Mode": "mixed", "Bound": int64(math.MaxInt64)}
			if last != nil {
				envelope["Last"] = last
			}
			body, err := json.Marshal(envelope)
			require.NoError(t, err)
			_, _, err = decodeReadCursor(append([]byte(readTokenPrefix), body...), queue, math.MaxInt64, true)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestReadCursorRequiresMatchingBound(t *testing.T) {
	t.Parallel()
	queue := dlq.QueueKey{CategoryID: 1, SourceCluster: "source", TargetCluster: "target"}
	for _, tc := range []struct {
		name         string
		bound        any
		includeBound bool
		messageID    int64
		valid        bool
	}{
		{name: "missing"},
		{name: "null", includeBound: true},
		{name: "changed", bound: 1, includeBound: true},
		{name: "past bound", bound: 0, includeBound: true, messageID: 1},
		{name: "explicit zero", bound: 0, includeBound: true, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			envelope := map[string]any{"Version": 1, "Queue": queue, "Mode": "mixed", "Last": readTuple{MessageID: tc.messageID}}
			if tc.includeBound {
				envelope["Bound"] = tc.bound
			}
			body, err := json.Marshal(envelope)
			require.NoError(t, err)
			cursor, isQueueV2, err := decodeReadCursor(append([]byte(readTokenPrefix), body...), queue, 0, true)
			if !tc.valid {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				return
			}
			require.NoError(t, err)
			require.False(t, isQueueV2)
			require.Equal(t, &readTuple{}, cursor)
		})
	}
}
