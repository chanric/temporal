package historydlq

import "go.temporal.io/server/chasm"

const (
	libraryName          = "historydlq"
	componentName        = "message"
	segmentComponentName = "segment"
	entryComponentName   = "entry"
)

type Library struct{ chasm.UnimplementedLibrary }

func NewNilLibrary() *Library { return &Library{} }

func (l *Library) Name() string { return libraryName }

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Message](componentName,
			chasm.WithBusinessIDAlias("HistoryDLQBusinessId"),
			chasm.WithSearchAttributes(categorySearchAttribute, sourceSearchAttribute, targetSearchAttribute,
				messageIDSearchAttribute, enqueuedAtSearchAttribute, pendingSearchAttribute)),
		chasm.NewRegistrableComponent[*Segment](segmentComponentName,
			chasm.WithBusinessIDAlias("HistoryDLQSegmentBusinessId"),
			chasm.WithSearchAttributes(categorySearchAttribute, sourceSearchAttribute, targetSearchAttribute, pendingSearchAttribute)),
		chasm.NewRegistrableComponent[*Entry](entryComponentName),
	}
}
