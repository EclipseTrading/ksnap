package main

import (
	"fmt"
	"github.com/akamensky/go-log"
	"io"
	"ksnap/internal/datastore"
	"ksnap/internal/kafka"
	"ksnap/internal/message"
	"ksnap/utils"
	"math"
	"strings"
	"sync"
)

func restore(brokers, topicNames []string, dataDir string, opts *utils.Options) {
	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			log.Fatal(r)
		}
	}()

	log.Infof("Loading all data...")

	// get kafka client
	c, err := kafka.NewClient(brokers)
	if err != nil {
		log.Fatal(err)
	}

	// load list of topics in snapshot
	topicsInBackup, err := datastore.ListTopics(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	var topicsNotInBackup []string

	// check that all topicNames are present in topicsInBackup
	for _, topicName := range topicNames {
		isFound := false
		for _, tib := range topicsInBackup {
			if topicName == tib {
				isFound = true
				break
			}
		}

		if !isFound {
			topicsNotInBackup = append(topicsNotInBackup, topicName)
		}
	}

	// list all topic-partitions in snapshot
	topicPartitionsInBackup := make(map[string][]int32)
	for _, tib := range topicsInBackup {
		parts, err := datastore.ListPartitions(dataDir, tib)
		if err != nil {
			log.Fatal(err)
		}

		topicPartitionsInBackup[tib] = parts
	}

	// load list of topics/partitions from kafka
	topicsNotInKafka := make([]string, 0)
	topicPartitionsInKafkaLess := make(map[string]int)
	topicPartitionsInKafkaMore := make(map[string]int)
	topicPartitionsInKafkaNotEmpty := make(map[string][]int32)
	partitions := make(map[kafka.Partition]datastore.RestoreDS)
	for _, topicName := range topicNames {
		tik := c.Topic(topicName)
		if tik == nil {
			topicsNotInKafka = append(topicsNotInKafka, topicName)
			continue
		}

		parts := tik.Partitions()

		if len(parts) < len(topicPartitionsInBackup[topicName]) {
			topicPartitionsInKafkaLess[topicName] = len(topicPartitionsInBackup[topicName]) - len(parts)
			continue
		}

		if len(parts) > len(topicPartitionsInBackup[topicName]) {
			topicPartitionsInKafkaMore[topicName] = len(parts) - len(topicPartitionsInBackup[topicName])
			continue
		}

		for _, part := range parts {
			if part.Size() > 0 {
				topicPartitionsInKafkaNotEmpty[topicName] = append(topicPartitionsInKafkaNotEmpty[topicName], part.Id())
			}

			partitions[part] = nil
		}
	}

	log.Infof("Validating conditions...")

	// if any of the error conditions happened. we should print them all and exit
	if len(topicsNotInBackup) > 0 || len(topicsNotInKafka) > 0 || len(topicPartitionsInKafkaLess) > 0 || len(topicPartitionsInKafkaMore) > 0 || len(topicPartitionsInKafkaNotEmpty) > 0 {
		// fail if topic list from CLI has topics not in this snapshot
		if len(topicsNotInBackup) > 0 {
			log.Errorf("Requested restore of topics not present in snapshot [ %s ]", strings.Join(topicsNotInBackup, ", "))
		}
		// fail if topic list from CLI has topics not found in Kafka
		if len(topicsNotInKafka) > 0 {
			log.Errorf("Topics to be restored, but not found in destination cluster [ %s ]", strings.Join(topicsNotInKafka, ", "))
		}
		// fail when topic in Kafka has less partitions than in snapshot directory, since we won't be able to restore messages
		if len(topicPartitionsInKafkaLess) > 0 {
			var msgs []string
			for key, val := range topicPartitionsInKafkaLess {
				msgs = append(msgs, fmt.Sprintf("topic %s lacks %d partition(s)", key, val))
			}
			log.Errorf("Topics to be restored, but have less partitions in destination cluster [ %s ]", strings.Join(msgs, ", "))
		}
		// fail when topic in Kafka has more partitions than in snapshot directory, since there will be issues with repartitioning
		if len(topicPartitionsInKafkaMore) > 0 {
			var msgs []string
			for key, val := range topicPartitionsInKafkaMore {
				msgs = append(msgs, fmt.Sprintf("topic %s has %d partition(s) more", key, val))
			}
			log.Errorf("Topics to be restored, but have more partitions in destination cluster [ %s ]", strings.Join(msgs, ", "))
		}
		// fail when destination partitions have messages in them (.Size() > 0)
		if len(topicPartitionsInKafkaNotEmpty) > 0 {
			var msgs []string
			for key, val := range topicPartitionsInKafkaNotEmpty {
				msgs = append(msgs, fmt.Sprintf("topic \"%s\" partitions (%s) are not empty", key, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(val)), ","), "[]")))
			}
			log.Errorf("Topics to be restored, but containing data [ %s ]", strings.Join(msgs, ", "))
		}

		log.Fatal("Sanity checks failed, please see and address all errors above")
	}

	// try to open datastore for each partition
	for partition := range partitions {
		ds, err := datastore.Open(dataDir, partition.Topic(), partition.Id())
		if err != nil {
			log.Fatal(err)
		}
		partitions[partition] = ds
	}

	log.Infof("Starting restore... this may take some time")

	// Restore topics one by one
	for p, ds := range partitions {
		log.Infof("Restoring topic [%s] partition [%d]...", p.Topic(), p.Id())

		log.Infof("Loading old consumer offsets")

		// Set consumer offsets
		oldOffsets := ds.GetConsumerOffsets()

		// Get write channel
		writer, done, offsetCh := p.WriteMessages()

		offsetChanges := make(map[int64]int64)
		var newTopicStart int64 = math.MaxInt64
		var newTopicEnd int64 = 0
		// start background process collecting new offsets
		lock := &sync.WaitGroup{}
		lock.Add(1)
		go func(offsetChanges map[int64]int64, offsetCh <-chan map[int64]int64) {
			for offsetChange := range offsetCh {
				for oldOffset, newOffset := range offsetChange {
					offsetChanges[oldOffset] = newOffset
					if newOffset > newTopicEnd {
						newTopicEnd = newOffset + 1
					}
					if newOffset < newTopicStart {
						newTopicStart = newOffset
					}
				}
			}
			lock.Done()
		}(offsetChanges, offsetCh)

		log.Infof("Writing [%d] messages...", ds.GetMessageCount())

		// Iterate over all messages
		for {
			msgBytes, err := ds.ReadMessage()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			msg, err := message.NewMessageFromBytes(msgBytes)
			if err != nil {
				log.Fatal(err)
			}

			writer <- msg
		}

		// Close datastore explicitly
		err := ds.Close()
		if err != nil {
			log.Fatal(err)
		}

		// Close writer channel and wait for all messages to be flushed to cluster
		close(writer)
		<-done

		// Wait till we finished reading updated offsets
		lock.Wait()

		// Only set consumer offsets if there are messages
		if ds.GetMessageCount() > 0 {
			// Transform old offsets to new offsets
			newOffsets := make(map[string]kafka.Offset)

			log.Infof("Transforming [%d] consumer offsets...", len(oldOffsets))

			for consumerGroup, oldOffset := range oldOffsets {
				if oldOffset.Offset() == 0 { // if offset is 0, set to 0
					newOffsets[consumerGroup] = kafka.NewOffset(0, oldOffset.Metadata())
				} else if oldOffset.Offset() == kafka.OffsetOldest { // if offset is at beginning, set to beginning
					newOffsets[consumerGroup] = kafka.NewOffset(kafka.OffsetOldest, oldOffset.Metadata())
				} else if oldOffset.Offset() <= p.StartOffset() { // if offset is at or before first message, set it to first message
					newOffsets[consumerGroup] = kafka.NewOffset(newTopicStart, oldOffset.Metadata())
				} else if oldOffset.Offset() == kafka.OffsetNewest { // if offset is at the end, set to the end
					newOffsets[consumerGroup] = kafka.NewOffset(kafka.OffsetNewest, oldOffset.Metadata())
				} else if oldOffset.Offset() >= p.EndOffset() { // if offset is after last message, set it to last message + 1
					newOffsets[consumerGroup] = kafka.NewOffset(newTopicEnd, oldOffset.Metadata())
				} else {
					newOffsets[consumerGroup] = kafka.NewOffset(offsetChanges[oldOffset.Offset()], oldOffset.Metadata())
				}
				log.Infof("%s at %d", consumerGroup, newOffsets[consumerGroup].Offset())
			}

			log.Infof("Restoring [%d] consumer offsets...", len(newOffsets))

			err = p.SetConsumerOffsets(newOffsets)
			if err != nil {
				log.Fatal(err)
			}
		}

		log.Infof("Finished restoring topic [%s] partition [%d]", p.Topic(), p.Id())
	}

	log.Info("Restore completed")
}
