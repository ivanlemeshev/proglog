// Package store defines the byte store and implements the file byte store.
package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// Store is an interface for the byte store.
type Store interface {
	// Append persists the given bytes to the store. Returns the number of
	// written bytes, the position where the store holds the record and an error.
	Append(record []byte) (n uint64, position uint64, err error)

	// Read returns the record stored at the given position.
	Read(position uint64) ([]byte, error)

	// ReadAt reads bytes of b length beginning at the offset.
	// It implements io.ReaderAt on the store type.
	ReadAt(b []byte, offset int64) (int, error)

	// Close persists any buffered data before closing the store.
	Close() error
}

// RecordSizeLength defines the number of bytes used to store the record length.
const RecordSizeLength = 8

// MaxRecordLength defines the maximum length of the single record.
const MaxRecordLength = 1 << RecordSizeLength

// ErrMaxRecordLength is returned if the record more the the maximum length.
var ErrMaxRecordLength = fmt.Errorf("the record to long: max length is %d", MaxRecordLength)

// store struct is a simple wrapper around a file to read and write bytes to it.
// This struct implements the Store interface.
type store struct {
	mu   sync.Mutex // to prevent cincurrent read/write to the file
	file *os.File
	buf  *bufio.Writer
	size uint64
}

// New returns a new store that wraps the given file.
func New(file *os.File) (Store, error) {
	// File could be not empty, so it is necessary to get its size. It equals
	// to 0 if the file is new and empty. The file size is used as the store size.
	fileStat, err := os.Stat(file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read the file stat: %w", err)
	}

	fileSize := uint64(fileStat.Size())

	store := &store{
		mu:   sync.Mutex{},
		file: file,
		size: fileSize,
		buf:  bufio.NewWriter(file),
	}

	return store, nil
}

// Append persists the given bytes to the store. Returns the number of written
// bytes, the position where the store holds the record in the file and an error.
func (s *store) Append(record []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(record) > MaxRecordLength {
		return 0, 0, ErrMaxRecordLength
	}

	// Remember current position to return at the end.
	position := s.size

	// fmt.Println(s.buf)

	// Write the record length to know the record size on reading.
	recordSize := uint64(len(record))
	if err := binary.Write(s.buf, binary.BigEndian, recordSize); err != nil {
		return 0, 0, fmt.Errorf("failed to write the record size: %w", err)
	}

	// fmt.Println(s.buf)

	// Write to the buffered writer instead of directly to the file to reduce
	// the number of system calls and improve performance.
	n, err := s.buf.Write(record)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write the record: %w", err)
	}

	// fmt.Println(s.buf)

	// Do not forget to add length of the record size.
	n += RecordSizeLength

	s.size += uint64(n)

	return uint64(n), position, nil
}

// Read returns the record stored at the given position.
func (s *store) Read(position uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to write all records from the buffer to disk before
	// reading from the file.
	if err := s.buf.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush the buffer: %w", err)
	}

	// Read the bytes that contain the record size.
	recordSize := make([]byte, RecordSizeLength)
	if _, err := s.file.ReadAt(recordSize, int64(position)); err != nil {
		return nil, fmt.Errorf("failed to read the record size: %w", err)
	}

	recordPosition := int64(position + RecordSizeLength)

	b := make([]byte, binary.BigEndian.Uint64(recordSize))
	if _, err := s.file.ReadAt(b, recordPosition); err != nil {
		return nil, fmt.Errorf("failed to read the record: %w", err)
	}

	return b, nil
}

// ReadAt reads bytes of b length from the file beginning at the offset.
func (s *store) ReadAt(b []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to write all records from the buffer to disk before
	// reading from the file.
	if err := s.buf.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush the buffer: %w", err)
	}

	n, err := s.file.ReadAt(b, offset)
	if err != nil {
		return 0, fmt.Errorf("failed to read: %w", err)
	}

	return n, nil
}

// Close persists any buffered data to file before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to write all records from the buffer to disk before
	// closing the file.
	if err := s.buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush the buffer: %w", err)
	}

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close the file: %w", err)
	}

	return nil
}
