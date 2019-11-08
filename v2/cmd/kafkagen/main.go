package main

import (
	"log"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/v2/kafkagen"
)

func main() {
	if err := commandeer.Run(kafkagen.NewMain()); err != nil {
		log.Fatal(err)
	}
}
