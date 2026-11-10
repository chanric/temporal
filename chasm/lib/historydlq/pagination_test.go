package historydlq

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestSegmentCursorRejectsMalformedEnvelopes(t *testing.T) {
	t.Parallel()
	for _, token := range []string{`{}`, `null`, `{"Version":2}`, `{"Version":1,"Unknown":true}`, `{} {}`} {
		t.Run(token, func(t *testing.T) {
			t.Parallel()
			_, _, ref, _ := metadataFixture(t)
			_, err := decodeSegmentCursor(ListEntriesRequest{Segment: ref, PageSize: 1, InclusiveMaxMessageID: math.MaxInt64, NextPageToken: []byte(token)})
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestSegmentCursorRequiresZeroNumericFields(t *testing.T) {
	t.Parallel()
	_, _, ref, _ := metadataFixture(t)
	for _, missing := range []string{"Bound", "MessageID"} {
		t.Run(missing, func(t *testing.T) {
			t.Parallel()
			req := ListEntriesRequest{Segment: ref, PageSize: 1, InclusiveMaxMessageID: 0}
			body := encodeSegmentCursor(req, MessageRef{MessageID: 0, EntryKey: "entry"})
			req.NextPageToken = body
			valid, err := decodeSegmentCursor(req)
			require.NoError(t, err)
			require.Equal(t, int64(0), valid.MessageID)
			var envelope map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &envelope))
			delete(envelope, missing)
			req.NextPageToken, err = json.Marshal(envelope)
			require.NoError(t, err)
			_, err = decodeSegmentCursor(req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}
