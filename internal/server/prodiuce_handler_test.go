package server_test

import (
	"net/http"
	"testing"

	"github.com/ivanlemeshev/proglog/internal/server"
	"github.com/steinfletcher/apitest"
)

func TestProduceHandler(t *testing.T) {
	t.Parallel()

	log := server.NewLog()

	handler := server.NewProduceHandler(log)

	apitest.New().
		HandlerFunc(handler).
		Post("/").
		JSON(`{"value": "cHJvZHVjZSBtZXNzYWdlIDA="}`). // "produce message 0"
		Expect(t).
		Status(http.StatusOK).
		Body(`{"offset":0}`).
		End()

	apitest.New().
		HandlerFunc(handler).
		Post("/").
		JSON(`{"value": "cHJvZHVjZSBtZXNzYWdlIDE="}`). // "produce message 1"
		Expect(t).
		Status(http.StatusOK).
		Body(`{"offset":1}`).
		End()

	apitest.New().
		HandlerFunc(handler).
		Post("/").
		JSON(`{"value": "cHJvZHVjZSBtZXNzYWdlIDI="}`). // "produce message 2"
		Expect(t).
		Status(http.StatusOK).
		Body(`{"offset":2}`).
		End()
}

func TestProduceHandler_BadRequest(t *testing.T) {
	t.Parallel()

	log := server.NewLog()

	handler := server.NewProduceHandler(log)

	apitest.New().
		HandlerFunc(handler).
		Post("/").
		Expect(t).
		Body(`{"error":"Bad request"}`).
		Status(http.StatusBadRequest).
		End()
}
