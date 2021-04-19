package server

import (
	"encoding/json"
	"errors"
	"net/http"
)

// ConsumeRequest is a consume request to read a record from the log.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse is a response on the consume request.
type ConsumeResponse struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type consumeHandler struct {
	log *Log
}

// NewConsumeHandler creates a new consume handler function.
func NewConsumeHandler(log *Log) http.HandlerFunc {
	handler := &consumeHandler{
		log: log,
	}

	return handler.handle
}

func (h *consumeHandler) handle(w http.ResponseWriter, r *http.Request) {
	var request ConsumeRequest

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Bad request")

		return
	}

	record, err := h.log.Read(request.Offset)
	if errors.Is(err, ErrOffsetNotFound) {
		writeErrorResponse(w, http.StatusNotFound, "Record not found")

		return
	}

	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, "Internal server error")

		return
	}

	resp := ConsumeResponse(record)

	writeResponse(w, http.StatusOK, resp)
}
