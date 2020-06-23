package kafka

import "github.com/Shopify/sarama"

type Topic interface {
	Name() string
	Partitions() []Partition
}

type topic struct {
	brokers    []string
	name       string
	partitions []Partition
}

func newTopic(saramaClient sarama.Client, brokers []string, name string) (Topic, error) {
	t := new(topic)
	t.brokers = brokers
	t.name = name

	partitions, err := saramaClient.Partitions(name)
	if err != nil {
		return nil, err
	}

	for _, partitionId := range partitions {
		p, err := newPartition(saramaClient, brokers, t.name, partitionId)
		if err != nil {
			return nil, err
		}

		t.partitions = append(t.partitions, p)
	}

	return t, nil
}

func (t *topic) Name() string {
	return t.name
}

func (t *topic) Partitions() []Partition {
	return t.partitions
}
