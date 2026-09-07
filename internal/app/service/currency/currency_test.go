package currency

import (
	"context"
	"errors"
	"testing"

	"github.com/Lagwick/worker-service/internal/app/entity"
	"github.com/Lagwick/worker-service/internal/app/repository"
)

// Фейки реализуют ровно те интерфейсы, от которых зависит сервис, — этого
// достаточно, чтобы проверить логику cache-aside без Redis и без сети.
var (
	_ RatesProvider           = (*fakeRates)(nil)
	_ repository.CurrencyRate = (*fakeRepo)(nil)
)

// fakeRates — источник "свежих" курсов (заглушка Fixer) с подсчётом вызовов:
// по числу обращений видно, сходили ли мы в API или обошлись кэшем.
type fakeRates struct {
	rates map[string]float64
	err   error
	calls int
}

func (f *fakeRates) GetRates(_ context.Context, _ string) (map[string]float64, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.rates, nil
}

// fakeRepo — кэш курсов поверх обычной map. Промах отдаётся доменным
// сентинелом entity.ErrRateNotFound, как того ждёт сервис.
type fakeRepo struct {
	store    map[string]float64
	setCalls int
}

func rateKey(from, to string) string { return from + ":" + to }

func (r *fakeRepo) GetRate(_ context.Context, from, to string) (float64, error) {
	if rate, ok := r.store[rateKey(from, to)]; ok {
		return rate, nil
	}
	return 0, entity.ErrRateNotFound
}

func (r *fakeRepo) SetRate(_ context.Context, from, to string, rate float64) error {
	if r.store == nil {
		r.store = make(map[string]float64)
	}
	r.store[rateKey(from, to)] = rate
	return nil
}

func (r *fakeRepo) SetRates(_ context.Context, _ string, _ map[string]float64) error {
	r.setCalls++
	return nil
}

func TestGetRate_SameCurrency(t *testing.T) {
	rates := &fakeRates{}
	repo := &fakeRepo{}
	svc := NewService(rates, repo)

	got, err := svc.GetRate(context.Background(), "EUR", "EUR")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1 {
		t.Fatalf("rate for from==to: want 1, got %v", got)
	}
	if rates.calls != 0 {
		t.Fatalf("provider must not be called for from==to, calls=%d", rates.calls)
	}
}

func TestGetRate_CacheHit(t *testing.T) {
	rates := &fakeRates{}
	repo := &fakeRepo{store: map[string]float64{"EUR:USD": 1.1}}
	svc := NewService(rates, repo)

	got, err := svc.GetRate(context.Background(), "EUR", "USD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1.1 {
		t.Fatalf("want 1.1 from cache, got %v", got)
	}
	if rates.calls != 0 {
		t.Fatalf("provider must not be called on cache hit, calls=%d", rates.calls)
	}
}

func TestGetRate_CacheMissFetchesAndFills(t *testing.T) {
	rates := &fakeRates{rates: map[string]float64{"USD": 1.2, "GBP": 0.85}}
	repo := &fakeRepo{} // пустой кэш -> промах
	svc := NewService(rates, repo)

	got, err := svc.GetRate(context.Background(), "EUR", "USD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1.2 {
		t.Fatalf("want 1.2 from provider, got %v", got)
	}
	if rates.calls != 1 {
		t.Fatalf("provider must be called once on miss, calls=%d", rates.calls)
	}
	if repo.setCalls != 1 {
		t.Fatalf("cache must be filled once after fetch, setCalls=%d", repo.setCalls)
	}
}

func TestGetRate_CurrencyNotFound(t *testing.T) {
	rates := &fakeRates{rates: map[string]float64{"USD": 1.2}} // нет XXX
	repo := &fakeRepo{}
	svc := NewService(rates, repo)

	_, err := svc.GetRate(context.Background(), "EUR", "XXX")
	if !errors.Is(err, entity.ErrFixerCurrencyNotFound) {
		t.Fatalf("want ErrFixerCurrencyNotFound, got %v", err)
	}
}

func TestGetRate_ProviderError(t *testing.T) {
	wantErr := errors.New("fixer unavailable")
	rates := &fakeRates{err: wantErr}
	repo := &fakeRepo{}
	svc := NewService(rates, repo)

	_, err := svc.GetRate(context.Background(), "EUR", "USD")
	if !errors.Is(err, wantErr) {
		t.Fatalf("provider error must propagate, got %v", err)
	}
}

func TestConvert(t *testing.T) {
	rates := &fakeRates{}
	repo := &fakeRepo{store: map[string]float64{"EUR:USD": 1.5}}
	svc := NewService(rates, repo)

	got, err := svc.Convert(context.Background(), 1000, "EUR", "USD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1500 {
		t.Fatalf("want 1000*1.5=1500, got %v", got)
	}
}
