package pdk

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"context"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/ctl"
)

type PilosaImporter interface {
	SetBit(bitmapID, profileID uint64, frame string)
	SetBitTimestamp(bitmapID, profileID uint64, frame string, timestamp time.Time)
	Close()
}

type Bit struct {
	row uint64
	col uint64
	ts  *time.Time
}

type ImportClient struct {
	BufferSize int

	channels map[string]chan Bit
	wg       sync.WaitGroup
}

func NewImportClient(host, index string, frames []string, bufsize int) *ImportClient {
	ic := &ImportClient{
		BufferSize: bufsize,
	}
	ic.channels = make(map[string]chan Bit, len(frames))
	for _, frame := range frames {
		ic.wg.Add(1)
		ic.channels[frame] = make(chan Bit, bufsize)
		go writer(ic.channels[frame], host, index, frame, ic.BufferSize, &ic.wg)
	}
	return ic
}

func (ic *ImportClient) SetBit(bitmapID, profileID uint64, frame string) {
	ic.channels[frame] <- Bit{row: bitmapID, col: profileID}
}

func (ic *ImportClient) SetBitTimestamp(bitmapID, profileID uint64, frame string, timestamp time.Time) {
	ic.channels[frame] <- Bit{row: bitmapID, col: profileID, ts: &timestamp}
}

func writer(bits <-chan Bit, host, index, frame string, bufsize int, wg *sync.WaitGroup) {
	defer wg.Done()
	pipeR, pipeW := io.Pipe()
	defer pipeW.Close()
	importer := ctl.ImportCommand{
		Host:       host,
		Index:      index,
		Frame:      frame,
		Paths:      []string{"-"},
		BufferSize: bufsize,

		CmdIO: pilosa.NewCmdIO(pipeR, os.Stdout, os.Stderr),
	}

	go func() {
		err := importer.Run(context.Background())
		if err != nil {
			log.Printf("Error with importer - frame: %v, err: %v", frame, err)
		}
	}()

	for bit := range bits {
		var err error
		if bit.ts == nil {
			_, err = pipeW.Write([]byte(fmt.Sprintf("%d,%d\n", bit.row, bit.col)))
		} else {
			_, err = pipeW.Write([]byte(
				fmt.Sprintf("%d,%d,%s\n", bit.row, bit.col, bit.ts.Format(pilosa.TimeFormat))))
		}
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
