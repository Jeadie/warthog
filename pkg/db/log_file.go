package db

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

// LogFile represents an append only, tab-spaced log file that refuses insertions above a fixed size.
type LogFile struct {
	maximumFileSize uint32
	file            *os.File
	isClosed        bool // To be added. Files can be marked closed requiring it to be open and
	// closed on each operation. This can be Set for less frequently accessed
	// files.
}

const defaultFilenamePrefix = "logFile"
const defaultExtension = "db"

// Set appends the value to the LogFile iff appending it would not exceed the maximum file size.
// Returns an error if appending the file would exceed the maximum file size or if there is an
// underlying i/o exception. Also returns the starting position of the insertion in the file. If
// an error occurs, the position returned is 0.
//
func (f *LogFile) Set(value string) (uint32, error) {
	if (uint32(len(value)) + f.size()) > f.maximumFileSize {
		return 0, errors.New("cannot add value, would exceed maximum file size")
	}

	// TODO: escape \t in value parameter.
	addition, err := f.file.WriteString(value + "\t")
	if err != nil || addition != len(value) + 1 {
		return 0, errors.New("error writing to file")
	}
	return f.size(), nil
}

// Get returns the value stored at the given index, up until the delimiter \t, exclusive.
// Returns an error the index could not be found in the file, or an error occurs reading from the
// index, up until the delimiter.
func (f *LogFile) Get(index uint32) (string, error) {
	if f.size() == 0 {
		return "", nil
	}
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
		// bufio's ReadString includes delimiter, must remove.
		return strings.TrimSuffix(result, "\t"), nil
	}
}

// Destroy deletes the underlying file associated with the LogFile.
func (f *LogFile) Destroy() {
	logFileName := f.file.Name()
	// Stop a LogFile being pointed to a file we didn't create, and then destroying this file.
	if !strings.Contains(logFileName, os.TempDir()) ||
		!strings.Contains(logFileName, defaultFilenamePrefix) ||
		logFileName[len(logFileName)-len(defaultExtension)-1:len(logFileName)-1] != defaultExtension {
		return
	}
	if err := os.Remove(logFileName); err != nil {
		fmt.Printf("could not remove file named: %s, for reason %v \n", logFileName, err)
	}
}

// ConstructLogFile constructs a LogFile object with a given maximum size. The file is created in
// the default temporary directory (which depends on the OS).
func ConstructLogFile(maximumFileSize uint32) (*LogFile, error) {
	var result LogFile
	temp, err := os.CreateTemp("", defaultFilenamePrefix+"*."+defaultExtension)
	if err != nil {
		return &result, err
	}
	result = LogFile{
		maximumFileSize: maximumFileSize,
		file:            temp,
	}
	return &result, nil
}

func (f *LogFile) size() uint32 {
	stat, _ := f.file.Stat()
	return uint32(stat.Size())
}

func (f *LogFile) getLogs() []string {
	file, _ := os.Open(f.file.Name())
	r := bufio.NewReader(file)
	logs := make([]string, 1)

	value, err := r.ReadString('\t')
	for err != nil {
		logs = append(logs, value)

		value, err = r.ReadString('\t')
	}
	return logs
}
