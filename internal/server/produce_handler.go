package server

import (
	"encoding/json"
	"net/http"
)

// ProduceRequest is a produce request to write a record into the log.
type ProduceRequest struct {
	Value []byte `json:"value"`
}

// ProduceResponse is a response on the produce request.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type produceHandler struct {
	log *Log
}

// NewProduceHandler creates a new produce handler.
func NewProduceHandler(log *Log) http.HandlerFunc {
	handler := &produceHandler{
		log: log,
	}

	return handler.handle
}

func (h *produceHandler) handle(w http.ResponseWriter, r *http.Request) {
	var request ProduceRequest

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Bad request")

		return
	}

	offset, err := h.log.Append(request.Value)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, "Internal server error")

		return
	}

	response := ProduceResponse{
		Offset: offset,
	}

	writeResponse(w, http.StatusOK, response)
}
