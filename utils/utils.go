package utils

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func SplitCommaString(commaString string) []string {
	var result []string
	parts := strings.Split(commaString, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		result = append(result, part)
	}
	return result
}

func IsDirEmpty(path string) (bool, error) {
	l, err := ioutil.ReadDir(path)
	if err != nil {
		return false, err
	}

	if len(l) > 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func IsDirExists(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	if !stat.IsDir() {
		return false, errors.New(fmt.Sprintf("%s is not a directory", path))
	}

	return true, nil
}

func IsFileExists(path string) (bool, error) {
	stat, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	if stat.IsDir() || !stat.Mode().IsRegular() {
		return false, errors.New(fmt.Sprintf("%s is not a regular file", path))
	}

	return true, nil
}
