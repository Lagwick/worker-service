package fixer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/Lagwick/worker-service/internal/app/config/section"
	"github.com/Lagwick/worker-service/internal/app/entity"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const requestTimeout = 10 * time.Second

// Client — HTTP-клиент Fixer API. Транспорт обёрнут otelhttp, поэтому
// исходящие запросы трассируются (no-op при выключенном OpenTelemetry).
type Client struct {
	httpClient *http.Client
	apiKey     string
	baseURL    string
}

type fixerResponse struct {
	Success bool               `json:"success"`
	Base    string             `json:"base"`
	Date    string             `json:"date"`
	Rates   map[string]float64 `json:"rates"`
	Error   *fixerError        `json:"error,omitempty"`
}

type fixerError struct {
	Code int    `json:"code"`
	Type string `json:"type"`
	Info string `json:"info"`
}

func NewClient(cfg section.ClientFixer) *Client {
	return &Client{
		httpClient: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			Timeout:   requestTimeout,
		},
		apiKey:  cfg.ApiKey,
		baseURL: cfg.BaseURL,
	}
}

func (c *Client) GetRates(ctx context.Context, base string) (map[string]float64, error) {
	u, err := url.Parse(c.baseURL + "/latest")
	if err != nil {
		return nil, fmt.Errorf("%w: build url: %w", entity.ErrFixerUnavailable, err)
	}

	params := url.Values{}
	params.Set("access_key", c.apiKey)
	params.Set("base", base)
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		u.String(),
		http.NoBody,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: create request: %w", entity.ErrFixerUnavailable, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", entity.ErrFixerUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"%w: unexpected status code %d",
			entity.ErrFixerUnavailable,
			resp.StatusCode,
		)
	}

	var body fixerResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("%w: %w", entity.ErrFixerInvalidResponse, err)
	}

	if !body.Success {
		return nil, mapFixerError(body.Error)
	}

	log.Info().
		Str("base", body.Base).
		Int("rates_count", len(body.Rates)).
		Msg("fixer rates received")

	return body.Rates, nil
}

// mapFixerError преобразует ошибку Fixer API в типизированную (errors.Is-совместимую).
func mapFixerError(fixerErr *fixerError) error {
	if fixerErr == nil {
		return entity.ErrFixerInvalidResponse
	}

	switch fixerErr.Code {
	case 101:
		return entity.ErrFixerInvalidApiKey
	case 104, 105:
		return entity.ErrFixerRateLimitExceeded
	default:
		return fmt.Errorf("%w: [%d] %s - %s", entity.ErrFixerInvalidResponse,
			fixerErr.Code, fixerErr.Type, fixerErr.Info)
	}
}
