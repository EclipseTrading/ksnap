package datastore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"ksnap/utils"
	"os"
)

type BackupDS interface {
	SetConsumerOffsets(map[string]Offset) error
	GetStartOffset() int64
	GetEndOffset() int64
	GetMessageCount() uint64
	WriteMessage([]byte) error
	Close() error
}

type RestoreDS interface {
	GetConsumerOffsets() map[string]Offset
	GetStartOffset() int64
	GetEndOffset() int64
	GetMessageCount() uint64
	ReadMessage() ([]byte, error)
	Close() error
}

type ds struct {
	readOnly     bool
	path         string
	currentChunk *chunk

	Topic           string                 `json:"topic"`
	Partition       int32                  `json:"partition"`
	StartOffset     int64                  `json:"start_offset"`
	EndOffset       int64                  `json:"end_offset"`
	MessageCount    uint64                 `json:"message_count"`
	Chunks          map[uint64]*chunk      `json:"chunks"`
	ConsumerOffsets map[string]*offsetType `json:"consumer_offsets"`
}

func (d *ds) GetStartOffset() int64 {
	return d.StartOffset
}

func (d *ds) GetEndOffset() int64 {
	return d.EndOffset
}

func (d *ds) GetMessageCount() uint64 {
	return d.MessageCount
}

func (d *ds) Close() error {
	for _, c := range d.Chunks {
		if c.fd != nil {
			err := c.Close()
			if err != nil {
				return err
			}
		}
	}

	if !d.readOnly {
		err := createMetadata(d.path, d)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *ds) loadChunk(id uint64) (*chunk, error) {
	// should already have chunk object in .Chunks
	// therefore can use that to load chunk
	var err error
	var c *chunk
	var ok bool

	if c, ok = d.Chunks[id]; !ok {
		return nil, os.ErrNotExist
	}

	err = c.Open(d.path, id)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (d *ds) createChunk(id uint64) (*chunk, error) {
	if _, ok := d.Chunks[id]; ok {
		return nil, errors.New(fmt.Sprintf("Cannot create chunk; chunk already exists"))
	}

	c, err := createChunk(d.path, id)
	if err != nil {
		return nil, err
	}

	// add chunk to the list
	d.Chunks[id] = c

	return c, nil
}

func ListTopics(root string) ([]string, error) {
	var result []string

	list, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}

	for _, f := range list {
		if !f.IsDir() {
			return nil, errors.New(fmt.Sprintf("Data directory is corrupted; [%s] is not a directory", f.Name()))
		}
		if !utils.IsTopicNameValid(f.Name()) {
			return nil, errors.New(fmt.Sprintf("Data directory is corrupted; [%s] is not a valid topic name", f.Name()))
		}

		result = append(result, f.Name())
	}

	return result, nil
}

func ListPartitions(root, topic string) ([]int32, error) {
	var result []int32

	topicPath, err := formatTopicPath(root, topic)
	if err != nil {
		return nil, err
	}

	list, err := ioutil.ReadDir(topicPath)
	if err != nil {
		return nil, err
	}

	for _, f := range list {
		if !f.IsDir() {
			return nil, errors.New(fmt.Sprintf("Data directory is corrupted; [%s] is not a directory", f.Name()))
		}

		id, err := parsePartitionId(f.Name())
		if err != nil {
			return nil, err
		}

		if id < 0 {
			return nil, errors.New(fmt.Sprintf("Data directory is corrupted; [%d] is not a valid partition ID", id))
		}

		result = append(result, id)
	}

	return result, nil
}
