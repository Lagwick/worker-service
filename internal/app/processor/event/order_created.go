package eprocessor

import (
	"context"
	"sync"

	"github.com/Lagwick/worker-service/internal/app/entity"
	ehandler "github.com/Lagwick/worker-service/internal/app/handler/event"
	"github.com/Lagwick/worker-service/internal/app/processor"
	"github.com/Lagwick/worker-service/internal/pkg/broker"
	"github.com/rs/zerolog/log"
)

type orderCreatedProc struct {
	h   ehandler.OrderCreated
	bus broker.Bus[entity.EventOrderCreated]
}

func NewOrderCreatedEventsCatcher(
	h ehandler.OrderCreated,
	bus broker.Bus[entity.EventOrderCreated],
) processor.Processor {
	return &orderCreatedProc{h, bus}
}

func (p *orderCreatedProc) StartAsync(ctx context.Context, wg *sync.WaitGroup) {
	if err := p.bus.Subscribe(ctx, wg, p.h.CallbackOrderCreated); err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to subscribe to order.created events")
	}

	log.Info().
		Msg("order.created subscription requested")
}
