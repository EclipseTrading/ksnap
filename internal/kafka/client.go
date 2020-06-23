package kafka

import (
	"github.com/Shopify/sarama"
)

type Client interface {
	Topics() []Topic
	Topic(string) Topic
}

type client struct {
	brokers []string
	topics  []Topic
}

func (c *client) Topics() []Topic {
	return c.topics
}

func (c *client) Topic(name string) Topic {
	for _, t := range c.topics {
		if t.Name() == name {
			return t
		}
	}

	return nil
}

func NewClient(brokers []string) (Client, error) {
	var err error

	c := new(client)

	c.brokers = brokers

	saramaClient, err := sarama.NewClient(c.brokers, getKafkaConfig())
	if err != nil {
		return nil, err
	}
	defer func() {
		err := saramaClient.Close()
		if err != nil {
			panic(err)
		}
	}()

	topicList, err := saramaClient.Topics()
	if err != nil {
		return nil, err
	}

	for _, topicName := range topicList {
		t, err := newTopic(saramaClient, brokers, topicName)
		if err != nil {
			return nil, err
		}

		c.topics = append(c.topics, t)
	}

	return c, nil
}
