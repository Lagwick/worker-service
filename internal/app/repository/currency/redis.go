package currency

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Lagwick/worker-service/internal/app/entity"
	"github.com/Lagwick/worker-service/internal/app/repository"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const rateKeyPrefix = "rates:"

// repoRedis — кэш курсов валют поверх Redis.
type repoRedis struct {
	client   *redis.Client
	cacheTTL time.Duration
}

func NewRepoFromRedis(client *redis.Client, cacheTTL time.Duration) repository.CurrencyRate {
	return &repoRedis{client: client, cacheTTL: cacheTTL}
}

func (r *repoRedis) buildKey(from, to string) string {
	return fmt.Sprintf("%s%s:%s", rateKeyPrefix, from, to)
}

func (r *repoRedis) GetRate(ctx context.Context, from, to string) (float64, error) {
	key := r.buildKey(from, to)

	value, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, entity.ErrRateNotFound
		}

		return 0, err
	}

	rate, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse currency rate: %w", err)
	}

	return rate, nil
}

func (r *repoRedis) SetRate(ctx context.Context, from, to string, rate float64) error {
	key := r.buildKey(from, to)
	value := strconv.FormatFloat(rate, 'f', -1, 64)

	if err := r.client.Set(ctx, key, value, r.cacheTTL).Err(); err != nil {
		return err
	}

	return nil
}

func (r *repoRedis) SetRates(ctx context.Context, from string, rates map[string]float64) error {
	pipe := r.client.Pipeline()

	for to, rate := range rates {
		value := strconv.FormatFloat(rate, 'f', -1, 64)

		pipe.Set(
			ctx,
			r.buildKey(from, to),
			value,
			r.cacheTTL,
		)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("set currency rates: %w", err)
	}

	log.Info().
		Ctx(ctx).
		Str("from", from).
		Int("rates_count", len(rates)).
		Msg("currency rates saved to redis")

	return nil
}
