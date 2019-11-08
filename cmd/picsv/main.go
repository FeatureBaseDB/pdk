package main

import (
	"log"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/csv"
)

func main() {
	if err := commandeer.Run(csv.NewMain()); err != nil {
		log.Fatal(err)
	}
}
