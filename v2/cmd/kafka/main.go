package main

import (
	"log"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/v2/kafka"
)

func main() {
	if err := commandeer.Run(kafka.NewMain()); err != nil {
		log.Fatal(err)
	}
}
