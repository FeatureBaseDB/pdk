package network

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"golang.org/x/net/context"

	"github.com/pilosa/pilosa/pilosactl"
)

type SetBit interface {
	SetBit(bitmapID, profileID uint64, frame string)
	Close()
}

type Bit struct {
	row uint64
	col uint64
}

type ImportClient struct {
	channels map[string]chan Bit
	wg       sync.WaitGroup
}

func NewImportClient(host, db string) *ImportClient {
	ic := &ImportClient{}
	ic.channels = map[string]chan Bit{
		netSrcFrame:      make(chan Bit, 0),
		netDstFrame:      make(chan Bit, 0),
		tranSrcFrame:     make(chan Bit, 0),
		tranDstFrame:     make(chan Bit, 0),
		netProtoFrame:    make(chan Bit, 0),
		transProtoFrame:  make(chan Bit, 0),
		appProtoFrame:    make(chan Bit, 0),
		hostnameFrame:    make(chan Bit, 0),
		methodFrame:      make(chan Bit, 0),
		contentTypeFrame: make(chan Bit, 0),
		userAgentFrame:   make(chan Bit, 0),
		packetSizeFrame:  make(chan Bit, 0),
		TCPFlagsFrame:    make(chan Bit, 0),
	}
	for frame, channel := range ic.channels {
		ic.wg.Add(1)
		go writer(channel, host, db, frame, &ic.wg)
	}
	return ic
}

func (ic *ImportClient) SetBit(bitmapID, profileID uint64, frame string) {
	ic.channels[frame] <- Bit{row: bitmapID, col: profileID}
}

func writer(bits <-chan Bit, host, db, frame string, wg *sync.WaitGroup) {
	defer wg.Done()
	pipeR, pipeW := io.Pipe()
	defer pipeW.Close()
	importer := pilosactl.ImportCommand{
		Host:       host,
		Database:   db,
		Frame:      frame,
		Paths:      []string{"-"},
		BufferSize: 10000000,
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
