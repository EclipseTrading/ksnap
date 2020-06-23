package datastore

import (
	"bytes"
	"errors"
	"fmt"
	"ksnap/utils"
	"os"
)

const maxChunkSize uint64 = 52428800 // 50 MB max chunk size

type chunk struct {
	fd           *os.File
	path         string
	size         uint64
	Id           uint64 `json:"id"`
	Md5sum       []byte `json:"md5sum"`
	MessageCount uint64 `json:"message_count"`
}

func (c *chunk) Open(location string, id uint64) error {
	if c.fd != nil {
		return nil
	}

	chunkPath, err := formatChunkPath(location, id)
	if err != nil {
		return err
	}

	c.path = chunkPath

	// open file for reading
	c.fd, err = os.Open(chunkPath)
	if err != nil {
		return err
	}

	return nil
}

func createChunk(location string, id uint64) (*chunk, error) {
	chunkPath, err := formatChunkPath(location, id)
	if err != nil {
		return nil, err
	}

	if ok, err := utils.IsFileExists(chunkPath); ok || err != nil {
		if err != nil {
			return nil, err
		}

		return nil, errors.New(fmt.Sprintf("Cannot create file [%s], file already exists", chunkPath))
	}

	fd, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	checksum, err := fileMd5(chunkPath)
	if err != nil {
		return nil, err
	}

	c := new(chunk)

	c.fd = fd
	c.path = chunkPath
	c.Id = id
	c.Md5sum = checksum
	c.MessageCount = 0

	return c, nil
}

func (c *chunk) Close() error {
	err := c.fd.Sync()
	if err != nil {
		return err
	}

	err = c.fd.Close()
	if err != nil {
		return err
	}

	err = c.updateMd5()
	if err != nil {
		return err
	}

	c.fd = nil

	return nil
}

func (c *chunk) validate() error {
	checksum, err := fileMd5(c.path)
	if err != nil {
		return err
	}

	if !bytes.Equal(checksum, c.Md5sum) {
		return errors.New(fmt.Sprintf("Chunk [%s] checksum verification failed", c.filename()))
	}

	return nil
}

func (c *chunk) Read(b []byte) (int, error) {
	return c.fd.Read(b)
}

func (c *chunk) Write(b []byte) (int, error) {
	n, err := c.fd.Write(b)
	c.size = c.size + uint64(n)
	if err != nil {
		return n, err
	}

	c.MessageCount = c.MessageCount + 1

	return n, nil
}

func (c *chunk) filename() string {
	return fmt.Sprintf("%010d", c.Id)
}

func (c *chunk) updateMd5() error {
	checksum, err := fileMd5(c.path)
	if err != nil {
		return err
	}

	c.Md5sum = checksum

	return nil
}
