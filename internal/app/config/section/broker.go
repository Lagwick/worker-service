package section

type (
	Broker struct {
		Kafka BrokerKafka
	}

	BrokerKafka struct {
		Addresses     []string `split_words:"true"`
		ConsumerGroup string   `split_words:"true"`
		ClientID      string   `split_words:"true"`

		ModelOrder BrokerKafkaModelOrder `split_words:"true"`
	}

	BrokerKafkaModelOrder struct {
		Created            BrokerKafkaModelOrderCreated            `split_words:"true"`
		DeliveryCalculated BrokerKafkaModelOrderDeliveryCalculated `split_words:"true"`
	}

	BrokerKafkaModelOrderDeliveryCalculated struct {
		Topic string `default:"order.delivery.calculated"`
	}

	BrokerKafkaModelOrderCreated struct {
		Topic         string `default:"order.created"`
		ConsumerGroup string `split_words:"true"`
	}
)
