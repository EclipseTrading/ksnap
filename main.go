package main

import (
	"github.com/akamensky/argparse"
	"github.com/akamensky/go-log"
	"ksnap/utils"
	"os"
)

// TODO: Implement authentication

func main() {
	p := argparse.NewParser("ksnap", "Create and restore point in time snapshots of Kafka data and other related tools")

	brokersString := p.String("b", "brokers", &argparse.Options{
		Default:  "localhost:9092",
		Required: false,
		Help:     "Comma-separated list of brokers in format `host:port'",
		Validate: utils.BrokersValidate,
	})

	topicsString := p.String("t", "topics", &argparse.Options{
		Required: true,
		Help:     "Comma-separated list of topics",
		Validate: utils.TopicsValidate,
	})

	dataDirString := p.String("d", "data", &argparse.Options{
		Required: true,
		Help:     "Directory for data snapshot",
		Validate: utils.DataDirValidate,
	})

	ignoreMissingTopicsFlag := p.Flag("", "ignore-missing-topics", &argparse.Options{
		Required: false,
		Help:     "Don't fail if topic name was provided in the arguments, but not found in cluster, print warning instead. Only used when creating snapshot",
		Default:  false,
	})

	createCmd := p.NewCommand("create", "Create a point-in-time snapshot of Apache Kafka data")
	restoreCmd := p.NewCommand("restore", "Restore a point-in-time snapshot of Apache Kafka data")

	err := p.Parse(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	// Main arguments
	brokers := utils.SplitCommaString(*brokersString)
	topics := utils.SplitCommaString(*topicsString)
	dataDir := *dataDirString

	// Additional arguments
	opts := &utils.Options{
		IgnoreMissingTopics: *ignoreMissingTopicsFlag,
	}

	if createCmd.Happened() {
		create(brokers, topics, dataDir, opts)
	} else if restoreCmd.Happened() {
		restore(brokers, topics, dataDir, opts)
	} else {
		log.Fatal("A valid sub-command is required check -h|--help for available sub-commands")
	}
}
