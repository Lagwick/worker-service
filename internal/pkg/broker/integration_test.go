//go:build integration

package broker_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/Lagwick/worker-service/internal/pkg/broker"
	"github.com/Lagwick/worker-service/internal/pkg/broker/codec"
)

type testEvent struct {
	ID  int    `json:"id"`
	Val string `json:"val"`
}

func testBrokers() []string {
	if v := os.Getenv("BROKER_TEST_ADDRESSES"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9094"}
}

func newAdmin(t *testing.T) sarama.ClusterAdmin {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	admin, err := sarama.NewClusterAdmin(testBrokers(), cfg)
	if err != nil {
		t.Fatalf("new cluster admin: %v", err)
	}
	return admin
}

func createTopic(t *testing.T, admin sarama.ClusterAdmin, name string, partitions int32) {
	t.Helper()
	err := admin.CreateTopic(name, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		t.Fatalf("create topic %q: %v", name, err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(name) })
}

func newClient(t *testing.T, group string) *broker.KafkaClient {
	t.Helper()
	client, err := broker.NewKafkaClient(broker.KafkaConfig{
		Addresses:     testBrokers(),
		ConsumerGroup: group,
	})
	if err != nil {
		t.Fatalf("new kafka client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestIntegration_ProducerConsumer(t *testing.T) {
	admin := newAdmin(t)
	defer admin.Close()

	topic := fmt.Sprintf("test.pc.%d", time.Now().UnixNano())
	createTopic(t, admin, topic, 1)

	group := "test-pc-" + topic
	client := newClient(t, group)
	bus := broker.MustKafkaBus[testEvent](client, codec.NewCodecJson[testEvent](), topic, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	const n = 5
	received := make(chan testEvent, n)
	err := bus.Subscribe(ctx, &wg, func(_ context.Context, ev *testEvent, _ []broker.Header) error {
		received <- *ev
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// OffsetNewest: даём группе вступить и получить партиции до отправки.
	time.Sleep(5 * time.Second)

	for i := 0; i < n; i++ {
		if err := bus.Send(ctx, &testEvent{ID: i, Val: fmt.Sprintf("v%d", i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	seen := make(map[int]string)
	deadline := time.After(20 * time.Second)
	for len(seen) < n {
		select {
		case ev := <-received:
			seen[ev.ID] = ev.Val
		case <-deadline:
			t.Fatalf("timeout: received %d/%d", len(seen), n)
		}
	}

	for i := 0; i < n; i++ {
		if seen[i] != fmt.Sprintf("v%d", i) {
			t.Fatalf("event %d: got %q", i, seen[i])
		}
	}

	cancel()
	wg.Wait()
}

type keyedTestEvent struct {
	GUID string `json:"guid"`
	Val  string `json:"val"`
}

func (e keyedTestEvent) EventId() string { return e.GUID }

func TestIntegration_MessageKeyFromEventId(t *testing.T) {
	admin := newAdmin(t)
	defer admin.Close()

	topic := fmt.Sprintf("test.key.%d", time.Now().UnixNano())
	createTopic(t, admin, topic, 1)

	client := newClient(t, "test-key-"+topic)
	const wantKey = "order-guid-123"
	bus := broker.MustKafkaBus[keyedTestEvent](client, codec.NewCodecJson[keyedTestEvent](), topic, "g")

	if err := bus.Send(context.Background(), &keyedTestEvent{GUID: wantKey, Val: "x"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	consumer, err := sarama.NewConsumer(testBrokers(), cfg)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("consume partition: %v", err)
	}
	defer pc.Close()

	select {
	case msg := <-pc.Messages():
		if string(msg.Key) != wantKey {
			t.Fatalf("message key = %q, want %q", msg.Key, wantKey)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIntegration_CriticalErrorReprocess(t *testing.T) {
	admin := newAdmin(t)
	defer admin.Close()

	topic := fmt.Sprintf("test.retry.%d", time.Now().UnixNano())
	createTopic(t, admin, topic, 1)

	group := "test-retry-" + topic
	client := newClient(t, group)
	bus := broker.MustKafkaBus[testEvent](client, codec.NewCodecJson[testEvent](), topic, group)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	warmup := make(chan struct{}, 1)
	var boomAttempts int64
	err := bus.Subscribe(ctx, &wg, func(_ context.Context, ev *testEvent, _ []broker.Header) error {
		if ev.Val == "warmup" {
			select {
			case warmup <- struct{}{}:
			default:
			}
			return nil
		}
		if atomic.AddInt64(&boomAttempts, 1) == 1 {
			return fmt.Errorf("transient failure on first delivery")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// OffsetNewest: даём группе вступить, затем коммитим базовый offset успешным
	// сообщением — без него rebalance после ошибки уехал бы на newest и пропустил bad-сообщение.
	time.Sleep(5 * time.Second)
	if err := bus.Send(ctx, &testEvent{ID: 0, Val: "warmup"}); err != nil {
		t.Fatalf("send warmup: %v", err)
	}
	select {
	case <-warmup:
	case <-time.After(20 * time.Second):
		t.Fatal("warmup message was not processed")
	}
	time.Sleep(3 * time.Second) // ждём авто-коммит baseline-offset

	// Сообщение падает на первой доставке -> ConsumeClaim выходит -> rebalance -> переобработка.
	if err := bus.Send(ctx, &testEvent{ID: 1, Val: "boom"}); err != nil {
		t.Fatalf("send boom: %v", err)
	}

	deadline := time.After(40 * time.Second)
	for atomic.LoadInt64(&boomAttempts) < 2 {
		select {
		case <-deadline:
			t.Fatalf("message was not reprocessed after critical error (boomAttempts=%d)", atomic.LoadInt64(&boomAttempts))
		case <-time.After(200 * time.Millisecond):
		}
	}

	cancel()
	wg.Wait()
}

func TestIntegration_ConsumerGroupBalancing(t *testing.T) {
	admin := newAdmin(t)
	defer admin.Close()

	topic := fmt.Sprintf("test.cg.%d", time.Now().UnixNano())
	createTopic(t, admin, topic, 3)
	group := "test-cg-" + topic

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	var total, c1, c2 int64

	sub := func(client *broker.KafkaClient, counter *int64) {
		bus := broker.MustKafkaBus[testEvent](client, codec.NewCodecJson[testEvent](), topic, group)
		err := bus.Subscribe(ctx, &wg, func(_ context.Context, _ *testEvent, _ []broker.Header) error {
			atomic.AddInt64(counter, 1)
			atomic.AddInt64(&total, 1)
			return nil
		})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}

	// Два отдельных клиента в одной группе — как два инстанса воркера.
	sub(newClient(t, group), &c1)
	sub(newClient(t, group), &c2)

	// Ждём вступления обоих и распределения партиций (rebalance).
	time.Sleep(8 * time.Second)

	prodBus := broker.MustKafkaBus[testEvent](newClient(t, group), codec.NewCodecJson[testEvent](), topic, group)
	const n = 30
	for i := 0; i < n; i++ {
		if err := prodBus.Send(ctx, &testEvent{ID: i, Val: fmt.Sprintf("v%d", i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	deadline := time.After(25 * time.Second)
	for atomic.LoadInt64(&total) < n {
		select {
		case <-deadline:
			t.Fatalf("timeout: total %d/%d (c1=%d c2=%d)", atomic.LoadInt64(&total), n, c1, c2)
		case <-time.After(200 * time.Millisecond):
		}
	}

	got1, got2 := atomic.LoadInt64(&c1), atomic.LoadInt64(&c2)
	if got1+got2 != n {
		t.Fatalf("total mismatch: c1=%d c2=%d sum!=%d", got1, got2, n)
	}
	if got1 == 0 || got2 == 0 {
		t.Fatalf("load not balanced across group: c1=%d c2=%d", got1, got2)
	}
	t.Logf("consumer group balanced: c1=%d c2=%d (total=%d)", got1, got2, n)

	cancel()
	wg.Wait()
}
