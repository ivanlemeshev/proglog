package main

import (
	"log"

	"github.com/ivanlemeshev/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080") // TODO: make the port configurable
	log.Fatal(srv.ListenAndServe())
}
