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

	tt := []struct {
		name         string
		requestBody  string
		responseBody string
	}{
		{
			"Consume message 0",
			`{"offset":0}`,
			`{"offset":0,"value":"Y29uc3VtZSBtZXNzYWdlIDA="}`,
		},
		{
			"Consume message 1",
			`{"offset":1}`,
			`{"offset":1,"value":"Y29uc3VtZSBtZXNzYWdlIDE="}`,
		},
		{
			"Consume message 2",
			`{"offset":2}`,
			`{"offset":2,"value":"Y29uc3VtZSBtZXNzYWdlIDI="}`,
		},
	}

	for _, tc := range tt {
		testCase := tc

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apitest.New().
				HandlerFunc(handler).
				Get("/").
				JSON(testCase.requestBody).
				Expect(t).
				Status(http.StatusOK).
				Body(testCase.responseBody).
				End()
		})
	}

	t.Run("Bad request", func(t *testing.T) {
		t.Parallel()

		apitest.New().
			HandlerFunc(handler).
			Get("/").
			Expect(t).
			Body(`{"error":"Bad request"}`).
			Status(http.StatusBadRequest).
			End()
	})

	t.Run("Not found", func(t *testing.T) {
		t.Parallel()

		apitest.New().
			HandlerFunc(handler).
			Get("/").
			JSON(`{"offset":123}`).
			Expect(t).
			Body(`{"error":"Record not found"}`).
			Status(http.StatusNotFound).
			End()
	})
}
