package pdk

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"context"

	"github.com/pilosa/pilosa/pilosactl"
)

type PilosaImporter interface {
	SetBit(bitmapID, profileID uint64, frame string)
	Close()
}

type Bit struct {
	row uint64
	col uint64
}

type ImportClient struct {
	BufferSize int

	channels map[string]chan Bit
	wg       sync.WaitGroup
}

func NewImportClient(host, db string, frames []string, bufsize int) *ImportClient {
	ic := &ImportClient{
		BufferSize: bufsize,
	}
	ic.channels = make(map[string]chan Bit, len(frames))
	for _, frame := range frames {
		ic.wg.Add(1)
		ic.channels[frame] = make(chan Bit, bufsize/2)
		go writer(ic.channels[frame], host, db, frame, ic.BufferSize, &ic.wg)
	}
	return ic
}

func (ic *ImportClient) SetBit(bitmapID, profileID uint64, frame string) {
	ic.channels[frame] <- Bit{row: bitmapID, col: profileID}
}

func writer(bits <-chan Bit, host, db, frame string, bufsize int, wg *sync.WaitGroup) {
	defer wg.Done()
	pipeR, pipeW := io.Pipe()
	defer pipeW.Close()
	importer := pilosactl.ImportCommand{
		Host:       host,
		Database:   db,
		Frame:      frame,
		Paths:      []string{"-"},
		BufferSize: bufsize,
		Stdin:      pipeR,
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
	}

	go func() {
		err := importer.Run(context.Background())
		if err != nil {
			log.Printf("Error with importer - frame: %v, err: %v", frame, err)
		}
	}()

	for bit := range bits {
		_, err := pipeW.Write([]byte(fmt.Sprintf("%d,%d\n", bit.row, bit.col)))
		if err != nil {
			log.Printf("Error writing to import pipe frame: %s, err: %v", frame, err)
		}
	}
}

func (ic *ImportClient) Close() {
	for _, c := range ic.channels {
		close(c)
	}
	log.Println("Waiting for all import client subroutines to complete")
	ic.wg.Wait()
	log.Println("Import client closed")
}
