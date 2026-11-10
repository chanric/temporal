package historydlq

import (
	"slices"

	commonspb "go.temporal.io/server/api/common/v1"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/persistence"
)

const (
	readBackendQueueV2 = iota
	readBackendCHASM
)

type mixedReadCandidate struct {
	tuple readTuple
	task  *commonspb.HistoryDLQTask
}

func mergeReadCandidates(queueV2Tasks []persistence.RawHistoryTask, chasmMessages []dlq.StoredMessage) []mixedReadCandidate {
	candidates := make([]mixedReadCandidate, 0, len(queueV2Tasks)+len(chasmMessages))
	for _, task := range queueV2Tasks {
		candidates = append(candidates, mixedReadCandidate{tuple: readTuple{MessageID: task.MessageMetadata.ID, Backend: readBackendQueueV2}, task: queueV2TaskToProto(task)})
	}
	for _, message := range chasmMessages {
		ref := message.Ref
		candidates = append(candidates, mixedReadCandidate{tuple: readTuple{MessageID: ref.MessageID, Backend: readBackendCHASM, BusinessID: ref.BusinessID, RunID: ref.RunID, EntryKey: ref.EntryKey}, task: &commonspb.HistoryDLQTask{Metadata: &commonspb.HistoryDLQTaskMetadata{MessageId: ref.MessageID}, Payload: message.Task}})
	}
	slices.SortFunc(candidates, func(a, b mixedReadCandidate) int { return compareTuple(a.tuple, b.tuple) })
	return candidates
}
