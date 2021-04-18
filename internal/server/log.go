// Package server implements commit log
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
	var log Log

	return &log
}

// Append adds a new record to the log.
func (c *Log) Append(value []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	offset := uint64(len(c.records))

	record := Record{
		Value:  value,
		Offset: offset,
	}

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
	Value  []byte
	Offset uint64
}
