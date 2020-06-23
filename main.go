package main

import (
	"github.com/akamensky/argparse"
	"github.com/akamensky/go-log"
	"ksnap/utils"
	"os"
)

// TODO: Implement consumer offset restoration
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

	backupCmd := p.NewCommand("backup", "Create a point-in-time snapshot of Apache Kafka data")
	restoreCmd := p.NewCommand("restore", "Restore a point-in-time snapshot of Apache Kafka data")

	err := p.Parse(os.Args)
	if err != nil {
		log.Fatal(err)
	}

	brokers := utils.SplitCommaString(*brokersString)
	topics := utils.SplitCommaString(*topicsString)
	dataDir := *dataDirString

	if backupCmd.Happened() {
		backup(brokers, topics, dataDir)
	} else if restoreCmd.Happened() {
		restore(brokers, topics, dataDir)
	} else {
		log.Fatal("A valid sub-command is required check -h|--help for available sub-commands")
	}
}
