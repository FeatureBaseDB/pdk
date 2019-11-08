package main

import (
	"log"

	"github.com/jaffee/commandeer/pflag"
	"github.com/pilosa/pdk/v2/kafka"
)

func main() {
	m := kafka.NewMain()
	if err := pflag.LoadEnv(m, "CONSUMER_", nil); err != nil {
		log.Fatal(err)
	}
	if err := m.Run(); err != nil {
		log.Fatal(err)
	}
}
