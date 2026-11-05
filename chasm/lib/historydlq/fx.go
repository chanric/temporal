package historydlq

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func register(registry *chasm.Registry, library *Library) error { return registry.Register(library) }

var Module = fx.Module("chasm.lib.historydlq", fx.Provide(NewNilLibrary), fx.Invoke(register))
