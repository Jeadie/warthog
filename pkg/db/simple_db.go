package db

import (
	"errors"
	"fmt"
)

// SimpleDB represents an in-memory key value database with a fixed number of entries,
type SimpleDB struct {
	table       map[string]string
	capacity    int8
	currentSize int8
}

// Set the key-value association into the SimpleDB. Returns true if the key was stored
// successfully, false otherwise.
func (db *SimpleDB) Set(key string, value string) bool {
	// if error, key not found -> Set would breach capacity
	if _, err := db.Get(key); err == nil && (db.currentSize >= db.capacity) {
		return false
	}
	db.table[key] = value
	fmt.Printf("key |%s| -> %s\n", key, db.table[key])
	return true
}

// Get the value associated with the key provided. Returns the stored value if present, otherwise
// returns a non-nil error.
func (db *SimpleDB) Get(key string) (string, error) {
	if value, ok := db.table[key]; ok {
		return value, nil
	} else {
		return value, errors.New("key not found")
	}
}

// ConstructSimpleDB creates a SimpleDB object with required capacity.
func ConstructSimpleDB(capacity int8) *SimpleDB {
	table := make(map[string]string)
	return &SimpleDB{
		table:       table,
		capacity:    capacity,
		currentSize: 0}
}
