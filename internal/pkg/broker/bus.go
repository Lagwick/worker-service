package broker

import (
	"context"
	"sync"
)

type Header struct {
	Key   string
	Value string
}

type MessageHandler[T any] func(ctx context.Context, msg *T, headers []Header) error

type Bus[T any] interface {
	Send(ctx context.Context, msg *T, headers ...Header) error
	Subscribe(ctx context.Context, wg *sync.WaitGroup, handler MessageHandler[T]) error
	QueueName() string
	Close() error
}
