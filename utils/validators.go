package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	topicNameRegex *regexp.Regexp
)

func init() {
	var err error

	topicNameRegex, err = regexp.Compile("^[a-zA-Z0-9._-]{3,249}$")
	if err != nil {
		panic(err)
	}
}

func IsTopicNameValid(name string) bool {
	if !topicNameRegex.MatchString(name) {
		return false
	}

	return true
}

func DataDirValidate(args []string) error {
	// validate that data dir is indeed a directory
	if len(args) != 1 {
		return errors.New("too many arguments for --data")
	}

	path, err := filepath.Abs(strings.TrimSpace(args[0]))
	if err != nil {
		return err
	}

	i, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New(fmt.Sprintf("data directory \"%s\" does not exist", path))
		} else {
			return err
		}
	}

	if !i.IsDir() {
		return errors.New(fmt.Sprintf("\"%s\" is not a directory", i))
	}

	return nil
}

func TopicsValidate(args []string) error {
	// validate that topics is indeed comma-separate list of valid topic names
	if len(args) != 1 {
		return errors.New("too many arguments for comma-separated list of topics")
	}

	topicsList := SplitCommaString(args[0])

	for _, topic := range topicsList {
		if len(topic) == 0 {
			return errors.New("invalid comma-separated list of topics")
		}

		if topic == "__consumer_offsets" {
			return errors.New("cannot directly use internal topic __consumer_offsets")
		}

		if !topicNameRegex.MatchString(topic) {
			return errors.New(fmt.Sprintf("\"%s\" is not a valid topic name", topic))
		}
	}

	return nil
}

func BrokersValidate(args []string) error {
	// validate that brokers is indeed comma-separate list of host:port
	if len(args) != 1 {
		return errors.New("too many arguments for comma-separated list of brokers")
	}

	brokersList := SplitCommaString(args[0])

	for _, broker := range brokersList {
		if len(broker) == 0 {
			return errors.New("invalid comma-separated list of brokers")
		}
	}

	return nil
}

func TopicValidate(args []string) error {
	// validate that topics is indeed comma-separate list of valid topic names
	if len(args) != 1 {
		return errors.New("too many arguments for comma-separated list of topics")
	}

	topicName := args[0]

	if len(topicName) == 0 {
		return errors.New("invalid topic name")
	}

	if topicName == "__consumer_offsets" {
		return errors.New("cannot directly use internal topic __consumer_offsets")
	}

	if !topicNameRegex.MatchString(topicName) {
		return errors.New(fmt.Sprintf("\"%s\" is not a valid topic name", topicName))
	}

	return nil
}
