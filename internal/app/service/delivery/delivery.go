package delivery

import (
	"context"
	"fmt"
	"math"

	"github.com/Lagwick/worker-service/internal/app/service"
)

const (
	// baseCurrency — валюта, в которой задан тариф доставки.
	baseCurrency = "EUR"
	// baseDeliveryPrice — базовая стоимость доставки в копейках (10 EUR).
	baseDeliveryPrice = 1000
)

// srv считает стоимость доставки в валюте заказа.
type srv struct {
	currency service.Currency
}

func NewService(currency service.Currency) service.Delivery {
	return &srv{currency: currency}
}

// Calculate возвращает стоимость доставки в копейках в валюте currency.
func (s *srv) Calculate(ctx context.Context, currency string) (int64, error) {
	price, err := s.currency.Convert(ctx, baseDeliveryPrice, baseCurrency, currency)
	if err != nil {
		return 0, fmt.Errorf("convert delivery price to %s: %w", currency, err)
	}

	return int64(math.Round(price)), nil
}
