package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer creates a new HTTP server.
func NewHTTPServer(addr string) *http.Server {
	log := NewLog()

	r := mux.NewRouter()
	r.HandleFunc("/", NewProduceHandler(log)).Methods("POST")
	r.HandleFunc("/", NewConsumeHandler(log)).Methods("GET")

	var server http.Server
	server.Addr = addr
	server.Handler = r

	return &server
}

// ErrorResponse is a response on error.
type ErrorResponse struct {
	Error string `json:"error"`
}

func writeResponse(w http.ResponseWriter, code int, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")

	_, err = w.Write(data)
	if err != nil {
		log.Println("Failed to write HTTP response:", err)
	}
}

func writeErrorResponse(w http.ResponseWriter, code int, err string) {
	response := ErrorResponse{
		Error: err,
	}

	writeResponse(w, code, response)
}
