package kafka

import "github.com/Shopify/sarama"

func getKafkaConfig() *sarama.Config {
	// Common settings
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.ClientID = "ksnap"

	// Consumer settings
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = false

	// Producer settings
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Admin settings

	return config
}

var OffsetOldest = sarama.OffsetOldest
var OffsetNewest = sarama.OffsetNewest
