package historydlq

import "time"

// Eligible compares an enqueue timestamp with a job's captured cutoff.
func Eligible(enqueuedAt, enqueueTimeCutoff time.Time) bool {
	return !enqueuedAt.IsZero() && !enqueueTimeCutoff.IsZero() && !enqueuedAt.After(enqueueTimeCutoff)
}
