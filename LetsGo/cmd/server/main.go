package main

import (
	"log"
	"utwoo.com/DistributedServicesWithGo/LetsGo/internal/server"
)

func main() {
	srv := server.NewHttpServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
