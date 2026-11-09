package historydlq

import (
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.uber.org/fx"
)

func registerLibrary(registry *chasm.Registry, library *Library) error {
	return registry.Register(library)
}

var ClientModule = fx.Module(
	"chasm.lib.historydlq.client",
	fx.Provide(historydlqpb.NewHistoryDLQServiceLayeredClient),
	fx.Provide(NewClient),
)

var Module = fx.Module(
	"chasm.lib.historydlq",
	ClientModule,
	fx.Provide(newServiceHandler),
	fx.Provide(newLibrary),
	fx.Invoke(registerLibrary),
)
