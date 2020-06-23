package datastore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

func Open(root, topic string, partition int32) (RestoreDS, error) {
	path, err := formatDsPath(root, topic, partition)
	if err != nil {
		return nil, err
	}

	// check if path is a directory
	if err := isDir(path); err != nil {
		return nil, err
	}

	// create datastore object
	d := new(ds)

	d.readOnly = true
	d.path = path

	// Load metadata
	err = loadMetadata(d.path, d)
	if err != nil {
		return nil, err
	}

	// Check metadata is valid
	if d.Topic != topic {
		return nil, errors.New(fmt.Sprintf("Invalid metadata, expected topic [%s], but got [%s]", topic, d.Topic))
	}
	if d.Partition != partition {
		return nil, errors.New(fmt.Sprintf("Invalid metadata, expected partition [%d], but got [%d]", partition, d.Partition))
	}
	for _, c := range d.Chunks {
		c.path, err = formatChunkPath(d.path, c.Id)
		if err != nil {
			return nil, err
		}

		err = c.validate()
		if err != nil {
			return nil, err
		}
	}

	// Everything is good, so can return it
	return d, nil
}

func (d *ds) ReadMessage() ([]byte, error) {
	var err error

	if d.currentChunk == nil {
		d.currentChunk, err = d.loadChunk(0)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, io.EOF
			} else {
				return nil, err
			}
		}
	}

	// Read message length
	msgLengthBytes := make([]byte, 8)
	n, err := d.currentChunk.Read(msgLengthBytes)
	if err != nil {
		if err == io.EOF {
			d.currentChunk, err = d.loadChunk(d.currentChunk.Id + 1)
			if err != nil {
				if os.IsNotExist(err) {
					return nil, io.EOF
				} else {
					return nil, err
				}
			} else {
				return d.ReadMessage()
			}
		} else {
			return nil, err
		}
	}
	if n != 8 {
		return nil, errors.New(fmt.Sprintf("Failed to Read [%d] bytes of message size from chunk [%d] at [%s]", n, d.currentChunk.Id, d.path))
	}

	msgLength := binary.BigEndian.Uint64(msgLengthBytes)

	// Read message data
	msgDataBytes := make([]byte, msgLength)
	n, err = d.currentChunk.Read(msgDataBytes)
	if err != nil {
		return nil, err
	}
	if uint64(n) != msgLength {
		return nil, errors.New(fmt.Sprintf("Failed to Read [%d] bytes of message size from chunk [%d] at [%s]", n, d.currentChunk.Id, d.path))
	}

	result := make([]byte, 8+msgLength)
	copy(result[0:8], msgLengthBytes[:])
	copy(result[8:8+msgLength], msgDataBytes[:])

	return result, nil
}

func (d *ds) GetConsumerOffsets() map[string]Offset {
	result := make(map[string]Offset)
	for group, offset := range d.ConsumerOffsets {
		result[group] = offset
	}
	return result
}
