package broker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Lagwick/worker-service/internal/pkg/broker"
	"github.com/Lagwick/worker-service/internal/pkg/broker/codec"
)

type orderEvent struct {
	OrderGUID  string `json:"order_guid"`
	TotalPrice int64  `json:"total_price"`
}

func TestCodecJSON_EncodeDecode(t *testing.T) {
	c := codec.NewCodecJson[orderEvent]()

	src := &orderEvent{OrderGUID: "guid-1", TotalPrice: 1500}
	data, err := c.Encode(src)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	const want = `{"order_guid":"guid-1","total_price":1500}`
	if string(data) != want {
		t.Fatalf("encode = %s, want %s", data, want)
	}

	got, err := c.Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if *got != *src {
		t.Fatalf("round trip = %+v, want %+v", got, src)
	}
}

func TestCodecJSON_DecodeInvalid(t *testing.T) {
	c := codec.NewCodecJson[orderEvent]()
	if _, err := c.Decode([]byte("{not json")); err == nil {
		t.Fatal("expected decode error for invalid json")
	}
}

func TestCoalesce(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want string
	}{
		{"first non-empty", []string{"a", "b"}, "a"},
		{"skip empty", []string{"", "b", "c"}, "b"},
		{"all empty", []string{"", "", ""}, ""},
		{"no args", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := broker.Coalesce(c.in...); got != c.want {
				t.Fatalf("Coalesce(%v) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestNotCriticalError(t *testing.T) {
	base := errors.New("bad payload")
	wrapped := broker.NotCriticalError(base)

	t.Run("nil stays nil", func(t *testing.T) {
		if broker.NotCriticalError(nil) != nil {
			t.Fatal("NotCriticalError(nil) must be nil")
		}
	})
	t.Run("detected as not critical", func(t *testing.T) {
		if !broker.IsNotCriticalError(wrapped) {
			t.Fatal("wrapped error must be not critical")
		}
	})
	t.Run("plain error is critical", func(t *testing.T) {
		if broker.IsNotCriticalError(base) {
			t.Fatal("plain error must be critical")
		}
		if broker.IsNotCriticalError(nil) {
			t.Fatal("nil must not be not critical")
		}
	})
	t.Run("preserves message", func(t *testing.T) {
		if wrapped.Error() != "bad payload" {
			t.Fatalf("Error() = %q, want %q", wrapped.Error(), "bad payload")
		}
	})
	t.Run("unwraps to original", func(t *testing.T) {
		if !errors.Is(wrapped, base) {
			t.Fatal("must unwrap to the original error")
		}
	})
	t.Run("detected through extra wrapping", func(t *testing.T) {
		outer := fmt.Errorf("handler: %w", wrapped)
		if !broker.IsNotCriticalError(outer) {
			t.Fatal("must detect not-critical through errors.As chain")
		}
	})
}

func TestNewBus_Validation(t *testing.T) {
	c := codec.NewCodecJson[orderEvent]()

	t.Run("nil client", func(t *testing.T) {
		if _, err := broker.NewBus[orderEvent](nil, c, "order.created", "g"); err == nil {
			t.Fatal("expected error for nil client")
		}
	})
	t.Run("empty topic", func(t *testing.T) {
		if _, err := broker.NewBus[orderEvent](&broker.KafkaClient{}, c, "", "g"); err == nil {
			t.Fatal("expected error for empty topic")
		}
	})
	t.Run("empty consumer group", func(t *testing.T) {
		if _, err := broker.NewBus[orderEvent](&broker.KafkaClient{}, c, "order.created", ""); err == nil {
			t.Fatal("expected error for empty consumer group")
		}
	})
	t.Run("success", func(t *testing.T) {
		bus, err := broker.NewBus[orderEvent](&broker.KafkaClient{}, c, "order.created", "g")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if bus.QueueName() != "order.created" {
			t.Fatalf("QueueName = %q, want %q", bus.QueueName(), "order.created")
		}
	})
}

func TestMustKafkaBus_Success(t *testing.T) {
	c := codec.NewCodecJson[orderEvent]()
	bus := broker.MustKafkaBus[orderEvent](&broker.KafkaClient{}, c, "order.created", "g")
	if bus.QueueName() != "order.created" {
		t.Fatalf("QueueName = %q", bus.QueueName())
	}
}

type keyedEvent struct{ GUID string }

func (e keyedEvent) EventId() string { return e.GUID }

func TestEventIdGetter_Contract(t *testing.T) {
	var _ broker.EventIdGetter = keyedEvent{GUID: "g1"}

	c := codec.NewCodecJson[keyedEvent]()
	bus, err := broker.NewBus[keyedEvent](&broker.KafkaClient{}, c, "order.created", "g")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bus.QueueName() != "order.created" {
		t.Fatalf("QueueName = %q", bus.QueueName())
	}
}

func TestBusMock_ImplementsBus(t *testing.T) {
	var _ broker.Bus[orderEvent] = broker.NewBusMock[orderEvent]("order.created")
}

func TestBusMock_RecordsMessages(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("order.created")

	if m.QueueName() != "order.created" {
		t.Fatalf("QueueName = %q", m.QueueName())
	}

	_ = m.Send(context.Background(), &orderEvent{OrderGUID: "g1"})
	_ = m.Send(context.Background(), &orderEvent{OrderGUID: "g2"},
		broker.Header{Key: "trace_id", Value: "abc"})

	sent := m.GetSentMessages()
	if len(sent) != 2 {
		t.Fatalf("len = %d, want 2", len(sent))
	}
	if sent[0].Msg.OrderGUID != "g1" {
		t.Fatalf("msg[0] = %+v", sent[0].Msg)
	}
	if len(sent[1].Headers) != 1 || sent[1].Headers[0].Key != "trace_id" || sent[1].Headers[0].Value != "abc" {
		t.Fatalf("headers = %v", sent[1].Headers)
	}
}

func TestBusMock_GetSentMessagesReturnsCopy(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")
	_ = m.Send(context.Background(), &orderEvent{OrderGUID: "g1"})

	got := m.GetSentMessages()
	got[0] = broker.SentMessage[orderEvent]{}

	if m.GetSentMessages()[0].Msg == nil {
		t.Fatal("internal slice must not be affected by caller mutation")
	}
}

func TestBusMock_Reset(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")
	_ = m.Send(context.Background(), &orderEvent{})
	m.Reset()
	if len(m.GetSentMessages()) != 0 {
		t.Fatal("Reset must clear recorded messages")
	}
}

func TestBusMock_Close(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")
	if err := m.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestBusMock_SendFuncOverride(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")
	m.SendFunc = func(context.Context, *orderEvent, ...broker.Header) error {
		return errors.New("kafka down")
	}

	if err := m.Send(context.Background(), &orderEvent{}); err == nil {
		t.Fatal("expected error from SendFunc")
	}
	if len(m.GetSentMessages()) != 1 {
		t.Fatal("message must be recorded even when SendFunc errors")
	}
}

func TestBusMock_SendFuncReceivesHeaders(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")
	var got []broker.Header
	m.SendFunc = func(_ context.Context, _ *orderEvent, headers ...broker.Header) error {
		got = headers
		return nil
	}

	_ = m.Send(context.Background(), &orderEvent{}, broker.Header{Key: "type", Value: "order.created"})
	if len(got) != 1 || got[0].Key != "type" {
		t.Fatalf("SendFunc headers = %v", got)
	}
}

func TestBusMock_SubscribeFunc(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")
	invoked := false
	m.SubscribeFunc = func(context.Context, *sync.WaitGroup, broker.MessageHandler[orderEvent]) error {
		invoked = true
		return nil
	}

	err := m.Subscribe(context.Background(), nil,
		func(context.Context, *orderEvent, []broker.Header) error { return nil })
	if err != nil {
		t.Fatalf("Subscribe = %v", err)
	}
	if !invoked {
		t.Fatal("SubscribeFunc must be invoked")
	}
}

func TestBusMock_ConcurrentSends(t *testing.T) {
	m := broker.NewBusMock[orderEvent]("t")

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Send(context.Background(), &orderEvent{})
		}()
	}
	wg.Wait()

	if len(m.GetSentMessages()) != n {
		t.Fatalf("got %d recorded sends, want %d", len(m.GetSentMessages()), n)
	}
}
