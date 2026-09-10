package entity

import (
	"github.com/Lagwick/worker-service/internal/pkg/broker"
	"github.com/gofrs/uuid"
	"github.com/rs/zerolog"
)

////////////////////////////////////////////////////////////////////////////////
///// EVENT MODEL //////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

type EventOrderCreated struct {
	OrderGUID  string                  `json:"order_guid"`
	UserGUID   *string                 `json:"user_guid,omitempty"`
	Currency   string                  `json:"currency"`
	TotalPrice int64                   `json:"total_price"`
	Items      []EventOrderCreatedItem `json:"items"`
	CreatedAt  string                  `json:"created_at"`
}

type EventOrderCreatedItem struct {
	ProductGUID string `json:"product_guid"`
	Quantity    int    `json:"quantity"`
	UnitPrice   int64  `json:"unit_price"`
}

func (e EventOrderCreated) MarshalZerologObject(ev *zerolog.Event) {
	ev.Str("order_guid", e.OrderGUID).
		Str("currency", e.Currency).
		Int64("total_price", e.TotalPrice).
		Int("items_count", len(e.Items))
}

// EventOrderDeliveryCalculated — результат расчёта доставки. Деньги в копейках
// (int64), как TotalPrice; currency — код валюты заказа.
type EventOrderDeliveryCalculated struct {
	OrderGUID     string `json:"order_guid"`
	DeliveryPrice int64  `json:"delivery_price"`
	Currency      string `json:"currency"`
	CalculatedAt  string `json:"calculated_at"`
}

func (e EventOrderDeliveryCalculated) MarshalZerologObject(ev *zerolog.Event) {
	ev.Str("order_guid", e.OrderGUID).
		Int64("delivery_price", e.DeliveryPrice).
		Str("currency", e.Currency)
}

////////////////////////////////////////////////////////////////////////////////
///// EVENT AUXILIARIES ////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

const (
	// BrokerHeaderKeyOrderEventType — логический тип события (роутинг/фильтрация
	// без декодирования payload).
	BrokerHeaderKeyOrderEventType = "type"
	// BrokerHeaderKeyOrderEventID — уникальный id сообщения для корреляции логов
	// (отличается от Kafka key, который отвечает за партиционирование).
	BrokerHeaderKeyOrderEventID = "event-id"

	BrokerHeaderValueOrderDeliveryCalculated = "order.delivery.calculated"
)

func BrokerHeaderOrderDeliveryCalculatedType() broker.Header {
	return broker.Header{
		Key:   BrokerHeaderKeyOrderEventType,
		Value: BrokerHeaderValueOrderDeliveryCalculated,
	}
}

func BrokerHeaderOrderDeliveryCalculatedEventID() broker.Header {
	return broker.Header{
		Key:   BrokerHeaderKeyOrderEventID,
		Value: uuid.Must(uuid.NewV4()).String(),
	}
}
