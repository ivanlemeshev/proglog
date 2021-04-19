package main

import (
	"log"

	"github.com/ivanlemeshev/proglog/internal/server"
)

func main() {
	const addr = ":8080" // TODO: make the address configurable
	srv := server.NewHTTPServer(addr)
	log.Fatal(srv.ListenAndServe())
}
