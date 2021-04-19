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

	tt := []struct {
		name         string
		requestBody  string
		responseBody string
	}{
		{
			"Produce message 0",
			`{"value": "cHJvZHVjZSBtZXNzYWdlIDA="}`,
			`{"offset":0}`,
		},
		{
			"Produce message 1",
			`{"value": "cHJvZHVjZSBtZXNzYWdlIDE="}`,
			`{"offset":1}`,
		},
		{
			"Produce message 2",
			`{"value": "cHJvZHVjZSBtZXNzYWdlIDI="}`,
			`{"offset":2}`,
		},
	}

	for _, tc := range tt {
		testCase := tc

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apitest.New().
				HandlerFunc(handler).
				Post("/").
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
			Post("/").
			Expect(t).
			Body(`{"error":"Bad request"}`).
			Status(http.StatusBadRequest).
			End()
	})
}
