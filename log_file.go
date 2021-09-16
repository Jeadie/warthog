package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

type LogFile struct {
	maximumFileSize uint32
	file            *os.File
	isClosed        bool // To be added. Files can be marked closed requiring it to be open and closed on each operation.
	// This can be set for less frequently accessed files.
}

func (f LogFile) set(value string) (uint32, error) {
	stat, _ := f.file.Stat()
	if (int64(len(value)) + stat.Size()) > int64(f.maximumFileSize) {
		return 0, errors.New("cannot add value, would breach maximum file size")
	}

	addition, err := f.file.WriteString(value + "\t")
	if err != nil {
		return 0, errors.New("error writing to file")
	}
	return uint32(stat.Size() + int64(addition)), nil
}

func (f LogFile) get(index uint32) (string, error) {
	if _, err := f.file.Seek(int64(index), 0); err != nil {
		return "", errors.New(
			fmt.Sprintf(
				"could not seek file to index %d. MaximumFileSize is %d",
				index,
				f.maximumFileSize))
	}
	result, err := bufio.NewReader(f.file).ReadString('\t')
	if err != nil {
		return "", err
	} else {
		return result, nil
	}

}
