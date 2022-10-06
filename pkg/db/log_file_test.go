package db

import (
	"os"
	"testing"
)

func TestLogFile_Get(t *testing.T) {
	tests := []struct{
		initialContent []string
		getIndex uint32
		maximumSize uint32
		result string
		err error
	}{
		{[]string{""}, 0, 100,"", nil},
		{[]string{"foo"}, 0, 100,"foo", nil},
		{[]string{"foo", "bar"}, 4, 100,"bar", nil},
		{[]string{""}, 7, 100,"", nil},
	}
	for i, tt := range tests {
		logFile := createLogFile(tt.maximumSize, tt.initialContent)
		result, err := logFile.Get(tt.getIndex)
		if result != tt.result {
			t.Errorf("Get() test %d has incorrect value. Expected %s. Received %s", i, tt.result, result)
		}
		if err != tt.err {
			t.Errorf("Get() test %d has incorrect error. Expected %+v. Received %+v", i, tt.err, err)
		}
	}
}

func createLogFile(maximumSize uint32, content []string) *LogFile {
	file, err := os.CreateTemp("", "")
	if err != nil {return nil}
	for _, s := range content {
		if s != "" {
			_, err := file.WriteString(s + "\t")
			if err != nil {return nil}
		}
	}
	return &LogFile{
		maximumFileSize: maximumSize,
		file:            file,
		isClosed:        false,
	}
}
