package broker

import (
	"fmt"

	"github.com/IBM/sarama"
)

type KafkaConfig struct {
	Addresses     []string
	ConsumerGroup string
	ClientID      string
}

type KafkaClient struct {
	client               sarama.Client
	producer             sarama.SyncProducer
	defaultConsumerGroup string
}

func NewKafkaClient(cfg KafkaConfig) (*KafkaClient, error) {
	clientID := Coalesce(cfg.ClientID, cfg.ConsumerGroup)
	defaultGroup := Coalesce(cfg.ConsumerGroup, cfg.ClientID)

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_8_0_0
	saramaCfg.ClientID = clientID

	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll

	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewClient(cfg.Addresses, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("broker: create sarama client: %w", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("broker: create sync producer: %w", err)
	}

	return &KafkaClient{
		client:               client,
		producer:             producer,
		defaultConsumerGroup: defaultGroup,
	}, nil
}

func (c *KafkaClient) Producer() sarama.SyncProducer {
	return c.producer
}

func (c *KafkaClient) NewConsumerGroup(group string) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroupFromClient(group, c.client)
}

func (c *KafkaClient) DefaultConsumerGroup() string {
	return c.defaultConsumerGroup
}

func (c *KafkaClient) Close() error {
	if c.producer != nil {
		if err := c.producer.Close(); err != nil {
			return fmt.Errorf("broker: close producer: %w", err)
		}
	}
	if c.client != nil && !c.client.Closed() {
		if err := c.client.Close(); err != nil {
			return fmt.Errorf("broker: close client: %w", err)
		}
	}
	return nil
}
