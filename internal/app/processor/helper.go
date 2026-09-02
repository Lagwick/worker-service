package processor

import (
	"context"
	"io"
	"sync"
)

// WatchForShutdown ожидает отмены контекста и закрывает closer (graceful shutdown).
func WatchForShutdown(ctx context.Context, wg *sync.WaitGroup, closer io.Closer) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = closer.Close()
	}()
}

// Wrap выполняет callback в горутине с поддержкой WaitGroup, проверяя отмену контекста.
func Wrap(ctx context.Context, wg *sync.WaitGroup, cb func(context.Context)) {
	if wg != nil {
		wg.Add(1)
	}

	go func() {
		if wg != nil {
			defer wg.Done()
		}
		select {
		case <-ctx.Done():
			return
		default:
			cb(ctx)
		}
	}()
}
