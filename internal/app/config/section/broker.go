package section

type (
	Broker struct {
		Kafka BrokerKafka
	}

	BrokerKafka struct {
		Addresses     []string `split_words:"true"`
		ConsumerGroup string   `split_words:"true"`
		ClientID      string   `split_words:"true"`
	}
)
