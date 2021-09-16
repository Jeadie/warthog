package main

import (
	"errors"
	"fmt"
)

type SimpleDB struct {
	table       map[string]string
	capacity    int8
	currentSize int8
}

func (db SimpleDB) set(key string, value string) bool {
	// if error, key not found -> set would breach capacity
	if _, err := db.get(key); err == nil && (db.currentSize >= db.capacity) {
		return false
	}
	db.table[key] = value
	fmt.Printf("key |%s| -> %s\n", key, db.table[key])
	return true
}

func (db SimpleDB) get(key string) (string, error) {
	if value, ok := db.table[key]; ok {
		return value, nil
	} else {
		fmt.Printf("Getter on key |%s| failed with error\n", key)
		return value, errors.New("key not found")
	}
}

func ConstructSimpleDB(capacity int8) SimpleDB {
	table := make(map[string]string)
	return SimpleDB{
		table:       table,
		capacity:    capacity,
		currentSize: 0}
}
