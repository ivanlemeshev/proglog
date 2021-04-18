package server

import (
	"fmt"
	"sync"
)

// ErrOffsetNotFound is an error on offest not found.
var ErrOffsetNotFound = fmt.Errorf("offset not found")

// Log is an implementation of commit log.
type Log struct {
	mu      sync.Mutex
	records []Record
}

// NewLog creates a new Log.
func NewLog() *Log {
	return &Log{}
}

// Append adds a new record to the log.
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)

	return record.Offset, nil
}

// Read reads a record form the log by the given offest.
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}

// Record is a record in the log.
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
