package main

import (
	"github.com/akamensky/go-log"
	"ksnap/internal/datastore"
	"ksnap/internal/kafka"
	"ksnap/utils"
)

func create(brokers, topicNames []string, dataDir string, opts *utils.Options) {
	// Data directory should be empty for new snapshot
	isEmpty, err := utils.IsDirEmpty(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	if !isEmpty {
		log.Fatal("data directory is not empty; aborting")
	}

	c, err := kafka.NewClient(brokers)
	if err != nil {
		log.Fatal(err)
	}

	log.Info("Connected to cluster")

	partitions := make([]kafka.Partition, 0)

	for _, name := range topicNames {
		t := c.Topic(name)
		if t == nil {
			if opts.IgnoreMissingTopics {
				// Should ignore topics that are missing in cluster
				// but still print warning
				log.Warnf("Topic [%s] does not exist in cluster", name)
				continue
			}
			log.Fatalf("Topic [%s] does not exist", name)
			return
		}

		for _, p := range t.Partitions() {
			log.Infof("Topic [%s], partition ID [%d], start offset [%d], end offset [%d], size [%d]", t.Name(), p.Id(), p.StartOffset(), p.EndOffset(), p.Size())

			partitions = append(partitions, p)
		}
	}

	if len(partitions) == 0 {
		log.Fatalf("No topics/partitions remains to make a snapshot")
		return
	}

	log.Info("Creating a snapshot")

	// Iterate over partitions
	for _, p := range partitions {
		log.Infof("Starting snapshot of topic [%s] partition [%d]", p.Topic(), p.Id())

		// Create datastore
		ds, err := datastore.Create(dataDir, p.Topic(), p.Id(), p.StartOffset(), p.EndOffset())
		if err != nil {
			panic(err)
		}

		log.Infof("Created datastore for topic [%s] partition [%d]", p.Topic(), p.Id())

		// get consumer offsets from Kafka and set it to datastore
		kafkaOffsets, err := p.GetConsumerOffsets()
		if err != nil {
			panic(err)
		}
		offsets := make(map[string]datastore.Offset)
		for group, offset := range kafkaOffsets {
			offsets[group] = datastore.NewOffset(offset.Offset(), offset.Metadata())
		}
		err = ds.SetConsumerOffsets(offsets)
		if err != nil {
			panic(err)
		}

		log.Infof("Got consumer offsets for topic [%s] partition [%d]", p.Topic(), p.Id())

		if p.Size() > 0 {
			for msg := range p.ReadMessages() {
				err := ds.WriteMessage(msg.EncodeBytes())
				if err != nil {
					panic(err)
				}
			}
		}

		log.Infof("Processed all messages for topic [%s] partition [%d]", p.Topic(), p.Id())

		err = ds.Close()
		if err != nil {
			panic(err)
		}

		log.Infof("Closed datastore for topic [%s] partition [%d]", p.Topic(), p.Id())

		log.Infof("Snapshot of topic [%s] partition [%d] done", p.Topic(), p.Id())
	}

	log.Info("Snapshot completed")
}
