package store_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ivanlemeshev/proglog/internal/log/store"
	"github.com/stretchr/testify/assert"
)

// nolint:gochecknoglobals
var ttAppend = []struct {
	name      string
	records   [][]byte
	withError bool
}{
	{
		name: "Several records",
		records: [][]byte{
			[]byte("record1"),
			[]byte("record2"),
			[]byte("record3"),
		},
		withError: false,
	},
	{
		name: "Empty record",
		records: [][]byte{
			[]byte(""),
			[]byte(""),
			[]byte(""),
		},
		withError: false,
	},
	{
		name: "Records with max length",
		records: [][]byte{
			make([]byte, store.MaxRecordLength),
			make([]byte, store.MaxRecordLength),
			make([]byte, store.MaxRecordLength),
		},
		withError: false,
	},
	{
		name: "Big record",
		records: [][]byte{
			make([]byte, store.MaxRecordLength+1),
		},
		withError: true,
	},
}

func TestStoreAppendRead(t *testing.T) {
	t.Parallel()

	for _, tc := range ttAppend {
		records := tc.records
		withError := tc.withError

		t.Run(tc.name, func(t *testing.T) {
			file, err := ioutil.TempFile("", "store_append_test")
			if err == nil {
				defer os.Remove(file.Name()) // nolint:errcheck
			}
			assert.Nil(t, err)

			s, err := store.New(file)
			if err == nil {
				defer s.Close() // nolint:errcheck
			}
			assert.Nil(t, err)

			t.Run("append", func(t *testing.T) {
				for i, r := range records {
					recordLength := uint64(len(r))
					expectedRecordedBytes := store.RecordSizeLength + recordLength
					expectedPosition := expectedRecordedBytes * uint64(i)
					n, position, err := s.Append(r)
					if withError {
						assert.Equal(t, uint64(0), n)
						assert.Equal(t, uint64(0), position)
						assert.NotNil(t, err)
					} else {
						assert.Equal(t, expectedRecordedBytes, n)
						assert.Equal(t, expectedPosition, position)
						assert.Nil(t, err)
					}
				}
			})
		})
	}
}

func TestStore_Read(t *testing.T) {
	t.Parallel()

	file, err := ioutil.TempFile("", "store_read_test")
	if err == nil {
		defer os.Remove(file.Name()) // nolint:errcheck
	}

	assert.Nil(t, err)

	s, err := store.New(file)
	if err == nil {
		defer s.Close() // nolint:errcheck
	}

	assert.Nil(t, err)

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	for _, r := range records {
		_, _, err := s.Append(r)
		assert.Nil(t, err)
	}

	t.Run("read records", func(t *testing.T) {
		for i, r := range records {
			recordLength := uint64(len(r))
			position := (store.RecordSizeLength + recordLength) * uint64(i)
			record, err := s.Read(position)
			assert.Equal(t, r, record)
			assert.Nil(t, err)
		}
	})

	t.Run("read with wrong position", func(t *testing.T) {
		_, err := s.Read(999999999999)
		assert.NotNil(t, err)
	})
}

func TestStore_ReadAt(t *testing.T) {
	t.Parallel()

	file, err := ioutil.TempFile("", "store_read_at_test")
	if err == nil {
		defer os.Remove(file.Name()) // nolint:errcheck
	}

	assert.Nil(t, err)

	s, err := store.New(file)
	if err == nil {
		defer s.Close() // nolint:errcheck
	}

	assert.Nil(t, err)

	records := [][]byte{
		[]byte("record1"),
		[]byte("record2"),
		[]byte("record3"),
	}

	for _, r := range records {
		_, _, err := s.Append(r)
		assert.Nil(t, err)
	}

	t.Run("read records", func(t *testing.T) {
		for i, r := range records {
			recordLength := uint64(len(r))
			record := make([]byte, len(r))
			offset := (store.RecordSizeLength+recordLength)*uint64(i) + store.RecordSizeLength
			_, err := s.ReadAt(record, int64(offset))
			assert.Equal(t, r, record)
			assert.Nil(t, err)
		}
	})

	t.Run("read with wrong offset", func(t *testing.T) {
		record := make([]byte, 10)
		_, err := s.ReadAt(record, 999999999999)
		assert.NotNil(t, err)
	})
}

func TestStore_Close(t *testing.T) {
	file, err := ioutil.TempFile("", "store_close_test")
	if err == nil {
		defer os.Remove(file.Name()) // nolint:errcheck
	}

	assert.Nil(t, err)

	s, err := store.New(file)
	assert.Nil(t, err)

	record := []byte("record")
	_, _, err = s.Append(record)
	assert.Nil(t, err)

	expectedBeforeSize := int64(0)
	beforeSize, err := fileSize(file.Name())
	assert.Nil(t, err)

	err = s.Close()
	assert.Nil(t, err)

	expectedAfterSize := int64(store.RecordSizeLength + len(record))
	afterSize, err := fileSize(file.Name())
	assert.Nil(t, err)

	assert.Equal(t, expectedBeforeSize, beforeSize)
	assert.Equal(t, expectedAfterSize, afterSize)
}

func fileSize(name string) (int64, error) {
	file, err := os.OpenFile(
		filepath.Clean(name),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return 0, err // nolint:wrapcheck
	}

	stat, err := file.Stat()
	if err != nil {
		return 0, err // nolint:wrapcheck
	}

	return stat.Size(), nil
}
