package main

import (
	"log"

	"github.com/jaffee/commandeer"
	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk/enterprise"
	"github.com/pkg/errors"
)

type Main struct {
	Pilosa string `help:"Pilosa enterprise node host:port."`
	Index  string `help:"Pilosa index to import into."`
}

func NewMain() *Main {
	return &Main{
		Pilosa: "localhost:20202", // enterprise default
		Index:  "examp",
	}
}

func (m *Main) Run() error {
	indexer, err := enterprise.SetupIndex([]string{m.Pilosa}, m.Index, nil, 100000)
	if err != nil {
		return errors.Wrap(err, "getting indexer")
	}

	// create frame (if necessary) and return bit iterator channel for that frame
	bitIter, err := indexer.Frame("aframe")
	if err != nil {
		return errors.Wrap(err, "getting frame iterator")
	}

	// create field (if necessary) and return a value iterator channel for that field
	fieldIter, err := indexer.Field("fieldframe", "afield")
	if err != nil {
		return errors.Wrap(err, "getting field iterator")
	}

	bitIter <- gopilosa.Bit{RowKey: "rowname1", ColumnKey: "columnname1"}
	fieldIter <- gopilosa.FieldValue{ColumnKey: "columnname1", Value: 88}

	// ... add more bits, perform queries, etc.

	// clean up nicely
	err = indexer.Close()
	return errors.Wrap(err, "closing indexer")
}

func main() {
	err := commandeer.Run(NewMain())
	if err != nil {
		log.Println(err)
	}
}
