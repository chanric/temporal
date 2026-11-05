package historydlq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEligible(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		cutoff time.Time
	}{
		{"3h", time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)},
		{"45m", time.Date(2026, 11, 4, 11, 15, 0, 0, time.UTC)},
		{"zero delay", start},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, input := range []struct {
				name       string
				enqueuedAt time.Time
				cutoff     time.Time
				want       bool
			}{
				{"boundary", tc.cutoff, tc.cutoff, true},
				{"newer", tc.cutoff.Add(time.Nanosecond), tc.cutoff, false},
				{"older", tc.cutoff.Add(-time.Nanosecond), tc.cutoff, true},
				{"missing enqueue time", time.Time{}, tc.cutoff, false},
				{"missing cutoff", tc.cutoff, time.Time{}, false},
				{"after admission", start.Add(time.Nanosecond), tc.cutoff, false},
			} {
				t.Run(input.name, func(t *testing.T) {
					t.Parallel()
					require.Equal(t, input.want, Eligible(input.enqueuedAt, input.cutoff))
				})
			}
		})
	}
}
