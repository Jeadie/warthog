package db

import (
	"errors"
	"fmt"
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
func (table *LogStreamTable) Set(key string, value string) bool {
	if ok:= table.appendToEnd(key, value); ok {
		return true
	}
	fmt.Println("Could not append to existing partition. Creating new partition")
	table.createNewPartition()
	return table.appendToEnd(key, value)
}

// Get a value associated with the key. Returns an error if the key is not in the in-memory index.
func (table *LogStreamTable) Get(key string) (string, error) {
	index, ok := table.inMemoryIndex[key]
	if !ok {
		return "", errors.New("error, key not found within in-memory index")
	}

	inFileIndex := index % table.partitionSize
	fileIndex := index / table.partitionSize
	file := table.diskPartitions[fileIndex]

	return file.Get(inFileIndex)
}

// ConstructLogSteamTable builds a LogStreamTable.
func ConstructLogStreamTable(chunk_size uint32) *LogStreamTable {
	index := make(map[string]uint32)
	table := LogStreamTable{
		inMemoryIndex:   index,
		diskPartitions: make(map[uint32]*LogFile),
		partitionSize:   chunk_size,
	}
	table.createNewPartition()
	return &table
}

// appendToEnd the key-value to the last log file. Returns true if the value was appended to disk
// and index set, false otherwise.
func (table *LogStreamTable) appendToEnd(key string, value string) bool {
	offset, err := table.diskPartitions[uint32(len(table.diskPartitions)-1)].Set(value)
	if err != nil {
		fmt.Printf("Failed to get disk partition offset for %s\n", key)
		return false
	}
	indexValue := table.partitionSize*uint32(len(table.diskPartitions)-1) + offset
	table.inMemoryIndex[key] = indexValue
	return true
}

// createNewPartition constructs another log file to the list of partitions used.
func (table *LogStreamTable) createNewPartition() {
	logFile, err := ConstructLogFile(table.partitionSize)
	if err != nil {
		fmt.Printf("could not create new partition %s\n", err)
		return
	}
	table.diskPartitions[uint32(len(table.diskPartitions))] = logFile
}
