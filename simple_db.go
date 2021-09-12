package main

import "errors"

type SimpleDB struct {
	table       map[string]string
	capacity    int8
	currentSize int8
}

func (db SimpleDB) set(key string, value string) error {
	// if error, key not found -> increase currentSize.
	if _, err := db.get(key); err == nil && (db.currentSize >= db.capacity) {
		return errors.New("SimpleDB has reached capacity")
	}
	db.table[key] = value
	return nil
}

func (db SimpleDB) get(key string) (string, error) {
	if value, err := db.table[key]; err {
		return value, nil
	} else {
		return value, errors.New("key not found")
	}

}

func ConstructSimpleDB(capacity int8) SimpleDB {
	table := make(map[string]string)
	return SimpleDB{
		table: table,
		capacity: capacity,
		currentSize: 0}
}
