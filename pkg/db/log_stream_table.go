package db

import (
	"errors"
	"fmt"
	"strings"
)

// LogStreamTable is an append-only log database with in-memory index.
type LogStreamTable struct {
	inMemoryIndex   map[string]uint32
	diskPartitions map[uint32]*LogFile
	partitionSize   uint32
}

// Set a value to the log file associated to the given key. Returns true if the key-value was set
// successfully, false otherwise.
//
// Will create additional partitions on disk if required.
func (t *LogStreamTable) Set(key string, value string) bool {
	if ok:= t.appendToEnd(key, value); ok {
		return true
	}
	fmt.Println("Could not append to existing partition. Creating new partition")
	t.createNewPartition()
	return t.appendToEnd(key, value)
}

// Get a value associated with the key. Returns an error if the key is not in the in-memory index.
func (t *LogStreamTable) Get(key string) (string, error) {
	index, ok := t.inMemoryIndex[key]
	if !ok {
		return "", errors.New("error, key not found within in-memory index")
	}

	inFileIndex := t.inFileOffset(index)
	fileIndex := t.fileIndex(index)
	file := t.diskPartitions[fileIndex]

	return file.Get(inFileIndex)
}

// ConstructLogSteamTable builds a LogStreamTable.
func ConstructLogStreamTable(chunk_size uint32) *LogStreamTable {
	index := make(map[string]uint32)
	t := LogStreamTable{
		inMemoryIndex:   index,
		diskPartitions: make(map[uint32]*LogFile),
		partitionSize:   chunk_size,
	}
	t.createNewPartition()
	return &t
}



func (t *LogStreamTable) inFileOffset(index uint32) uint32 {
	return index % t.partitionSize
}

func (t *LogStreamTable) fileIndex(index uint32) uint32 {
	return index/ t.partitionSize
}

func serialise(key string, value string) string {
	return fmt.Sprintf("%s||%s", key, value)
}

func deserialise(value string) (string, string) {
	s := strings.SplitN(value, "||", 2)
	return s[0], s[1]
}

// appendToEnd the key-value to the last log file. Returns true if the value was appended to disk
// and index set, false otherwise.
func (t *LogStreamTable) appendToEnd(key string, value string) bool {
	serialisedValue := serialise(key, value)
	offset, err := t.diskPartitions[uint32(len(t.diskPartitions)-1)].Set(serialisedValue)
	if err != nil {
		fmt.Printf("Failed to get disk partition offset for %s\n", key)
		return false
	}
	indexValue := t.partitionSize*uint32(len(t.diskPartitions)-1) + offset
	t.inMemoryIndex[key] = indexValue
	return true
}

// createNewPartition constructs another log file to the list of partitions used.
func (t *LogStreamTable) createNewPartition() {
	logFile, err := ConstructLogFile(t.partitionSize)
	if err != nil {
		fmt.Printf("could not create new partition %s\n", err)
		return
	}
	t.diskPartitions[uint32(len(t.diskPartitions))] = logFile
}
