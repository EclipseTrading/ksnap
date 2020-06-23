package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type Offset interface {
	Offset() int64
	Metadata() string
}

type offsetType struct {
	offset   int64
	metadata string
}

func (o *offsetType) Offset() int64 {
	return o.offset
}

func (o *offsetType) Metadata() string {
	return o.metadata
}

func NewOffset(offset int64, metadata string) Offset {
	return &offsetType{
		offset:   offset,
		metadata: metadata,
	}
}

func getConsumerOffsets(brokers []string, topic string, partition int32) (map[string]Offset, error) {
	c, err := sarama.NewClusterAdmin(brokers, getKafkaConfig())
	if err != nil {
		return nil, err
	}

	consumerGroups, err := c.ListConsumerGroups()
	if err != nil {
		return nil, err
	}

	result := make(map[string]Offset)

	for group := range consumerGroups {
		resp, err := c.ListConsumerGroupOffsets(group, map[string][]int32{topic: {partition}})
		if err != nil {
			return nil, err
		}

		for _, m := range resp.Blocks {
			for _, b := range m {
				if b.Offset == sarama.OffsetNewest {
					continue
				}
				result[group] = NewOffset(b.Offset, b.Metadata)
			}
		}
	}

	return result, nil
}

func setConsumerOffsets(brokers []string, topic string, partition int32, group string, offset Offset) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"group.id":          group,
	})
	if err != nil {
		return err
	}

	offsets := make([]kafka.TopicPartition, 0)
	o, err := kafka.NewOffset(offset.Offset())
	if err != nil {
		return err
	}
	m := offset.Metadata()

	offsets = append(offsets, kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    o,
		Metadata:  &m,
	})

	_, err = c.CommitOffsets(offsets)
	if err != nil {
		return err
	}

	return nil
}
