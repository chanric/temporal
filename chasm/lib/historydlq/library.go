package historydlq

import "go.temporal.io/server/chasm"

const (
	libraryName   = "historydlq"
	componentName = "message"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
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
	}
}
