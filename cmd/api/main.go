package main

import (
	"log"
	"net/http"
	"os"

	"github.com/ayushgawai/fleet-control-plane/internal/fleet"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	// Production wiring (docker-compose): swap fleet.NewStore/Bus/Queue for
	// Postgres/DynamoDB/Redis/Kafka/SQS adapters behind the same semantics.
	log.Println("fleet api listening on", addr)
	log.Fatal(http.ListenAndServe(addr, fleet.NewAPI().Handler()))
}
