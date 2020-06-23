package datastore

import (
	"errors"
	"fmt"
	"ksnap/utils"
	"os"
)

func Create(root, topic string, partition int32, startOffset, endOffset int64) (BackupDS, error) {
	// check root is a directory
	if err := isDir(root); err != nil {
		return nil, err
	}

	path, err := formatDsPath(root, topic, partition)
	if err != nil {
		return nil, err
	}

	// Create partition directory
	err = os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	if ok, err := utils.IsDirEmpty(path); !ok || err != nil {
		if err != nil {
			return nil, err
		}

		return nil, errors.New(fmt.Sprintf("Directory [%s] is not empty", path))
	}

	// create datastore object
	d := new(ds)

	d.readOnly = false
	d.path = path
	d.Topic = topic
	d.Partition = partition
	d.Chunks = make(map[uint64]*chunk)
	d.ConsumerOffsets = make(map[string]*offsetType)
	d.StartOffset = startOffset
	d.EndOffset = endOffset

	return d, nil
}

func (d *ds) WriteMessage(data []byte) error {
	var err error

	if d.currentChunk == nil {
		d.currentChunk, err = d.createChunk(0)
		if err != nil {
			return err
		}
	}

	// Check current chunk size + new data
	if d.currentChunk.size+uint64(len(data)) > maxChunkSize {
		oldChunk := d.currentChunk
		d.currentChunk, err = d.createChunk(d.currentChunk.Id + 1)
		if err != nil {
			return err
		}

		err = oldChunk.Close()
		if err != nil {
			return err
		}

		err = createMetadata(d.path, d)
		if err != nil {
			return err
		}
	}

	_, err = d.currentChunk.Write(data)
	if err != nil {
		return err
	}

	d.MessageCount = d.MessageCount + 1

	return nil
}

func (d *ds) SetConsumerOffsets(offsets map[string]Offset) error {
	d.ConsumerOffsets = make(map[string]*offsetType)
	for group, offset := range offsets {
		d.ConsumerOffsets[group] = &offsetType{
			OffsetV:   offset.Offset(),
			MetadataV: offset.Metadata(),
		}
	}

	return createMetadata(d.path, d)
}
