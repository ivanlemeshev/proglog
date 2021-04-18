package server_test

import (
	"net/http"
	"testing"

	"github.com/ivanlemeshev/proglog/internal/server"
	"github.com/steinfletcher/apitest"
)

func TestConsumeHandler(t *testing.T) {
	t.Parallel()

	log := server.NewLog()

	_, _ = log.Append([]byte("consume message 0")) // "Y29uc3VtZSBtZXNzYWdlIDA="
	_, _ = log.Append([]byte("consume message 1")) // "Y29uc3VtZSBtZXNzYWdlIDE="
	_, _ = log.Append([]byte("consume message 2")) // "Y29uc3VtZSBtZXNzYWdlIDI="

	handler := server.NewConsumeHandler(log)

	apitest.New().
		HandlerFunc(handler).
		Get("/").
		JSON(`{"offset":0}`).
		Expect(t).
		Status(http.StatusOK).
		Body(`{"offset":0,"value":"Y29uc3VtZSBtZXNzYWdlIDA="}`).
		End()

	apitest.New().
		HandlerFunc(handler).
		Get("/").
		JSON(`{"offset":1}`).
		Expect(t).
		Status(http.StatusOK).
		Body(`{"offset":1,"value":"Y29uc3VtZSBtZXNzYWdlIDE="}`).
		End()

	apitest.New().
		HandlerFunc(handler).
		Get("/").
		JSON(`{"offset":2}`).
		Expect(t).
		Status(http.StatusOK).
		Body(`{"offset":2,"value":"Y29uc3VtZSBtZXNzYWdlIDI="}`).
		End()
}

func TestConsumeHandler_BadRequest(t *testing.T) {
	t.Parallel()

	log := server.NewLog()

	handler := server.NewConsumeHandler(log)

	apitest.New().
		HandlerFunc(handler).
		Get("/").
		Expect(t).
		Body(`{"error":"Bad request"}`).
		Status(http.StatusBadRequest).
		End()
}

func TestConsumeHandler_NotFound(t *testing.T) {
	t.Parallel()

	log := server.NewLog()

	handler := server.NewConsumeHandler(log)

	apitest.New().
		HandlerFunc(handler).
		Get("/").
		JSON(`{"offset":1}`).
		Expect(t).
		Body(`{"error":"Record not found"}`).
		Status(http.StatusNotFound).
		End()
}
