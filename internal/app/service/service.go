package service

import "context"

type (
	// Currency — курсы валют и конвертация (cache-aside: Redis -> Fixer).
	Currency interface {
		GetRate(ctx context.Context, from, to string) (float64, error)
		Convert(ctx context.Context, amount float64, from, to string) (float64, error)
	}
)
