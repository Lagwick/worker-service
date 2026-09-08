package repository

import "context"

type (
	// CurrencyRate — кэш курсов валют.
	CurrencyRate interface {
		// GetRate возвращает курс from -> to из кэша.
		GetRate(ctx context.Context, from, to string) (float64, error)

		// SetRate сохраняет один курс from -> to.
		// Задел на точечное обновление; сейчас сервис заполняет кэш пачкой (SetRates).
		SetRate(ctx context.Context, from, to string, rate float64) error

		// SetRates сохраняет набор курсов относительно базовой валюты from.
		SetRates(ctx context.Context, from string, rates map[string]float64) error
	}
)
