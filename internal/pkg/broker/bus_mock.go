package broker

import (
	"context"
	"sync"
)

var _ Bus[any] = (*BusMock[any])(nil)

type SentMessage[T any] struct {
	Msg     *T
	Headers []Header
}

type BusMock[T any] struct {
	SendFunc      func(ctx context.Context, msg *T, headers ...Header) error
	SubscribeFunc func(ctx context.Context, wg *sync.WaitGroup, handler MessageHandler[T]) error

	TopicValue   string
	mu           sync.Mutex
	SentMessages []SentMessage[T]
}

func NewBusMock[T any](topic string) *BusMock[T] {
	return &BusMock[T]{
		TopicValue:   topic,
		SentMessages: make([]SentMessage[T], 0),
	}
}

func (m *BusMock[T]) Send(ctx context.Context, msg *T, headers ...Header) error {
	m.mu.Lock()
	m.SentMessages = append(m.SentMessages, SentMessage[T]{Msg: msg, Headers: headers})
	m.mu.Unlock()

	if m.SendFunc != nil {
		return m.SendFunc(ctx, msg, headers...)
	}
	return nil
}

func (m *BusMock[T]) Subscribe(
	ctx context.Context, wg *sync.WaitGroup, handler MessageHandler[T],
) error {
	if m.SubscribeFunc != nil {
		return m.SubscribeFunc(ctx, wg, handler)
	}
	return nil
}

func (m *BusMock[T]) QueueName() string {
	return m.TopicValue
}

func (m *BusMock[T]) Close() error {
	return nil
}

func (m *BusMock[T]) GetSentMessages() []SentMessage[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SentMessage[T], len(m.SentMessages))
	copy(out, m.SentMessages)
	return out
}

func (m *BusMock[T]) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SentMessages = make([]SentMessage[T], 0)
}
