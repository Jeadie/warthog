package db

// Database interface for a variety of key-value stores.
type Database interface {
	Get(string) (string, error)
	Set(string, string) bool
}
