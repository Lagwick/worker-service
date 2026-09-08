package currency

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lagwick/worker-service/internal/app/entity"
	"github.com/Lagwick/worker-service/internal/app/repository"
	"github.com/Lagwick/worker-service/internal/app/service"
	"github.com/rs/zerolog/log"
)

// RatesProvider — источник "свежих" курсов (Fixer API). Интерфейс, а не
// конкретный клиент, чтобы сервис не зависел от реализации HTTP-клиента.
type RatesProvider interface {
	GetRates(ctx context.Context, base string) (map[string]float64, error)
}

// srv реализует cache-aside: сначала кэш (Redis), при промахе — Fixer
// с заполнением кэша.
type srv struct {
	rates RatesProvider
	repo  repository.CurrencyRate
}

func NewService(rates RatesProvider, repo repository.CurrencyRate) service.Currency {
	return &srv{rates: rates, repo: repo}
}

func (s *srv) GetRate(ctx context.Context, from, to string) (float64, error) {
	if from == to {
		return 1, nil
	}

	rate, err := s.repo.GetRate(ctx, from, to)
	if err == nil {
		return rate, nil
	}

	if !errors.Is(err, entity.ErrRateNotFound) {
		log.Warn().
			Ctx(ctx).
			Err(err).
			Str("from", from).
			Str("to", to).
			Msg("failed to get currency rate from cache")
	}

	rates, err := s.rates.GetRates(ctx, from)
	if err != nil {
		return 0, fmt.Errorf("get rates from fixer: %w", err)
	}

	if err := s.repo.SetRates(ctx, from, rates); err != nil {
		log.Warn().
			Ctx(ctx).
			Err(err).
			Str("from", from).
			Msg("failed to cache currency rates")
	}

	rate, ok := rates[to]
	if !ok {
		return 0, entity.ErrFixerCurrencyNotFound
	}

	return rate, nil
}

func (s *srv) Convert(ctx context.Context, amount float64, from, to string) (float64, error) {
	rate, err := s.GetRate(ctx, from, to)
	if err != nil {
		return 0, err
	}
	return amount * rate, nil
}
