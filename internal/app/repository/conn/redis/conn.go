package rcredis

import (
	"context"
	"fmt"
	"time"

	"github.com/Lagwick/worker-service/internal/app/config/section"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Client встраивает *redis.Client — наружу доступны все команды go-redis напрямую.
type Client struct {
	*redis.Client
}

func NewClient(ctx context.Context, cfg section.RepositoryRedis) (*Client, error) {
	log.Info().
		Str("addr", cfg.Address).
		Int("db", cfg.DB).
		Msg("connecting to redis")

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := redisotel.InstrumentTracing(client); err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("instrument redis tracing: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("ping redis: %w", err)
	}

	log.Info().
		Str("addr", cfg.Address).
		Int("db", cfg.DB).
		Msg("connected to redis")

	return &Client{
		Client: client,
	}, nil
}
