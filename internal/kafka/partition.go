package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/akamensky/go-log"
	"ksnap/internal/message"
)

type Partition interface {
	Topic() string
	Id() int32
	StartOffset() int64
	EndOffset() int64
	Size() int64
	GetConsumerOffsets() (map[string]Offset, error)
	SetConsumerOffsets(map[string]Offset) error
	ReadMessages() <-chan message.Message
	WriteMessages() (chan<- message.Message, <-chan interface{}, <-chan map[int64]int64)
}

type partition struct {
	brokers []string
	topic   string
	id      int32
	start   int64
	end     int64
}

func newPartition(saramaClient sarama.Client, brokers []string, topic string, id int32) (Partition, error) {
	var err error
	p := new(partition)

	p.brokers = brokers
	p.topic = topic
	p.id = id

	p.start, err = saramaClient.GetOffset(p.topic, p.id, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	p.end, err = saramaClient.GetOffset(p.topic, p.id, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *partition) Topic() string {
	return p.topic
}

func (p *partition) Id() int32 {
	return p.id
}

func (p *partition) StartOffset() int64 {
	return p.start
}

func (p *partition) EndOffset() int64 {
	return p.end
}

func (p *partition) Size() int64 {
	return p.end - p.start
}

func (p *partition) GetConsumerOffsets() (map[string]Offset, error) {
	return getConsumerOffsets(p.brokers, p.topic, p.id)
}

func (p *partition) SetConsumerOffsets(offsets map[string]Offset) error {
	for consumerGroup, offset := range offsets {
		err := setConsumerOffsets(p.brokers, p.topic, p.id, consumerGroup, offset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *partition) ReadMessages() <-chan message.Message {
	result := make(chan message.Message)

	go func(p *partition, output chan<- message.Message) {
		defer func() {
			if r := recover(); r != nil {
				log.Fatalf("Error when transforming message from Kafka for topic [%s], partition [%d], the error was: %s", p.topic, p.id, r)
			}
		}()

		c, err := sarama.NewConsumer(p.brokers, getKafkaConfig())
		if err != nil {
			panic(err)
		}

		pc, err := c.ConsumePartition(p.topic, p.id, sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}

		lastOffset := p.EndOffset() - 1
		for kafkaMessage := range pc.Messages() {
			msgHeaders := make([]message.Header, len(kafkaMessage.Headers))
			for _, kafkaHeader := range kafkaMessage.Headers {
				msgHeaders = append(msgHeaders, message.NewHeader(kafkaHeader.Key, kafkaHeader.Value))
			}
			msg := message.NewMessage(kafkaMessage.Topic, kafkaMessage.Partition, kafkaMessage.Offset, kafkaMessage.Key, kafkaMessage.Value, kafkaMessage.Timestamp, kafkaMessage.BlockTimestamp, msgHeaders)

			output <- msg

			if kafkaMessage.Offset >= lastOffset {
				err = pc.Close()
				if err != nil {
					panic(err)
				}

				close(output)

				return
			}
		}

	}(p, result)

	return result
}

func (p *partition) WriteMessages() (chan<- message.Message, <-chan interface{}, <-chan map[int64]int64) {
	msgCh := make(chan message.Message)
	doneCh := make(chan interface{})
	offsetCh := make(chan map[int64]int64)

	sp, err := sarama.NewSyncProducer(p.brokers, getKafkaConfig())
	if err != nil {
		panic(err)
	}

	go func(p *partition, sp sarama.SyncProducer, ch <-chan message.Message, doneCh chan interface{}, offsetCh chan<- map[int64]int64) {
		for ksnapMessage := range ch {
			saramaMessage := &sarama.ProducerMessage{
				Topic:     ksnapMessage.Topic(),
				Key:       sarama.ByteEncoder(ksnapMessage.Key()),
				Value:     sarama.ByteEncoder(ksnapMessage.Value()),
				Headers:   nil,
				Metadata:  nil,
				Offset:    ksnapMessage.Offset(),
				Partition: ksnapMessage.Partition(),
				Timestamp: ksnapMessage.Timestamp(),
			}
			for _, header := range ksnapMessage.Headers() {
				saramaMessage.Headers = append(saramaMessage.Headers, sarama.RecordHeader{
					Key:   header.Key(),
					Value: header.Value(),
				})
			}

			responsePartition, responseOffset, err := sp.SendMessage(saramaMessage)
			if err != nil {
				panic(fmt.Sprintf("Error writing message to topic [%s] partition [%d]. Error was: %s", ksnapMessage.Topic(), ksnapMessage.Partition(), err.Error()))
			}

			// if written to different partition -- that is very bad
			if responsePartition != ksnapMessage.Partition() {
				panic(fmt.Sprintf("Error writing message to topic [%s] partition [%d], message was written to partition [%d] instead", ksnapMessage.Topic(), ksnapMessage.Partition(), responsePartition))
			}

			offsetMap := map[int64]int64{ksnapMessage.Offset(): responseOffset}
			offsetCh <- offsetMap
		}

		// SyncProducer MUST be closed in order to flush all internally cached messages
		err := sp.Close()
		if err != nil {
			panic(err)
		}

		close(doneCh)
		close(offsetCh)
	}(p, sp, msgCh, doneCh, offsetCh)

	return msgCh, doneCh, offsetCh
}
