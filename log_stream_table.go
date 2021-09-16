package main

import (
	"errors"
)

/*
 LogStreamTable is an append-only log database with in-memory index.
*/
type LogStreamTable struct {
	inMemoryIndex   map[string]uint32
	diskPartititons []LogFile
	partitionSize   uint32
}

func (table LogStreamTable) set(key string, value string) bool {
	if table.appendToEnd(key, value) {
		return true
	}
	table.createNewPartition()
	return table.appendToEnd(key, value)
}
func (table LogStreamTable) appendToEnd(key string, value string) bool {
	offset, err := table.diskPartititons[len(table.diskPartititons)-1].set(value)
	if err != nil {
		return false
	}
	table.inMemoryIndex[key] = table.partitionSize*uint32(len(table.diskPartititons)-1) + offset
	return true
}

func (table LogStreamTable) createNewPartition() {
	// TODO: Construct LogFile correctly.
	table.diskPartititons = append(table.diskPartititons, LogFile{})
}

func (table LogStreamTable) get(key string) (string, error) {
	index, ok := table.inMemoryIndex[key]
	if !ok {
		return "", errors.New("error, key not found within in-memory index")
	}
	inFileIndex := index % table.partitionSize
	fileIndex := index / table.partitionSize
	file := table.diskPartititons[fileIndex]
	return file.get(inFileIndex)
}

func ConstructLogStreamtable(chunk_size uint32) LogStreamTable {
	return LogStreamTable{
		inMemoryIndex:   make(map[string]uint32),
		diskPartititons: []LogFile{},
		partitionSize:   chunk_size,
	}
}
