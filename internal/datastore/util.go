package datastore

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

const metadataFilename = ".metadata"

func formatDsPath(root, topic string, partition int32) (string, error) {
	return filepath.Abs(fmt.Sprintf("%s/%s/%010d", root, topic, partition))
}

func formatTopicPath(root, topic string) (string, error) {
	return filepath.Abs(fmt.Sprintf("%s/%s", root, topic))
}

func parsePartitionId(partitionDirname string) (int32, error) {
	re, err := regexp.Compile("^[0-9]{10}$")
	if err != nil {
		return 0, err
	}

	if !re.MatchString(partitionDirname) {
		return 0, errors.New(fmt.Sprintf("[%s] is not valid name for partition directory", partitionDirname))
	}

	i, err := strconv.ParseInt(partitionDirname, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(i), nil
}

func formatChunkPath(dsPath string, chunkId uint64) (string, error) {
	return filepath.Abs(fmt.Sprintf("%s/%010d", dsPath, chunkId))
}

func isDir(path string) error {
	var result error

	if dir, err := os.Stat(path); err != nil || !dir.IsDir() {
		if err != nil {
			result = err
		} else {
			result = errors.New(fmt.Sprintf("\"%s\" is not a directory", path))
		}
	}

	return result
}

func loadMetadata(root string, v interface{}) error {
	content, err := ioutil.ReadFile(filepath.Clean(fmt.Sprintf("%s/%s", root, metadataFilename)))
	if err != nil {
		return err
	}
	err = json.Unmarshal(content, v)
	if err != nil {
		return err
	}

	return nil
}

func createMetadata(root string, v interface{}) error {
	fd, err := os.OpenFile(filepath.Clean(fmt.Sprintf("%s/%s", root, metadataFilename)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return err
	}

	_, err = fd.Write(data)
	if err != nil {
		return err
	}

	err = fd.Sync()
	if err != nil {
		return err
	}

	return fd.Close()
}

func fileMd5(filepath string) ([]byte, error) {
	//Open the passed argument and check for any error
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	//Open a new hash interface to write to
	hash := md5.New()

	//Copy the file in the hash interface and check for any error
	if _, err := io.Copy(hash, file); err != nil {
		return nil, err
	}

	// Close file
	err = file.Close()
	if err != nil {
		return nil, err
	}

	//Get the 16 bytes hash
	checksum := hash.Sum(nil)[:16]

	return checksum, nil
}
