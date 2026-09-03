package eorder

import (
	"context"
	"fmt"

	"github.com/Lagwick/worker-service/internal/app/entity"
	ehandler "github.com/Lagwick/worker-service/internal/app/handler/event"
	"github.com/Lagwick/worker-service/internal/pkg/broker"
	"github.com/rs/zerolog/log"
)

type handler struct{}

func NewHandler() ehandler.OrderCreated {
	return &handler{}
}

func (h *handler) CallbackOrderCreated(
	ctx context.Context,
	ev *entity.EventOrderCreated,
	_ []broker.Header,
) error {
	log.Info().
		Ctx(ctx).
		EmbedObject(ev).
		Msg("order created event received")

	if ev.OrderGUID == "" {
		return broker.NotCriticalError(
			fmt.Errorf("order_guid is empty"),
		)
	}

	return nil
}
