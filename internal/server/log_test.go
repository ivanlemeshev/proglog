package server_test

import (
	"testing"

	"github.com/lemeshev/proglog/internal/server"
	"github.com/stretchr/testify/assert"
)

func TestAppend(t *testing.T) {
	t.Parallel()

	l := server.NewLog()

	offset0, err := l.Append([]byte("first"))
	assert.Nil(t, err)
	assert.Equal(t, offset0, uint64(0))

	offset1, err := l.Append([]byte("second"))
	assert.Nil(t, err)
	assert.Equal(t, offset1, uint64(1))
}

func TestRead(t *testing.T) {
	t.Parallel()

	l := server.NewLog()

	firstValue := []byte("first")
	offset0, err := l.Append(firstValue)
	assert.Nil(t, err)

	secondValue := []byte("second")
	offset1, err := l.Append(secondValue)
	assert.Nil(t, err)

	r0, err := l.Read(offset0)
	assert.Nil(t, err)
	assert.Equal(t, firstValue, r0.Value)

	r1, err := l.Read(offset1)
	assert.Nil(t, err)
	assert.Equal(t, secondValue, r1.Value)

	_, err = l.Read(999999)
	assert.Equal(t, server.ErrOffsetNotFound, err)
}
