package pdk_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"testing"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa/server"
)

func TestSetupPilosa(t *testing.T) {
	s := MustNewRunningServer(t)
	host := "http://" + s.Server.Addr().String()

	frames := []pdk.FrameSpec{
		{
			Name:           "frame1",
			CacheType:      pcli.CacheTypeRanked,
			CacheSize:      17,
			InverseEnabled: true,
		},
		{
			Name:           "frame2",
			CacheType:      pcli.CacheTypeLRU,
			CacheSize:      19,
			InverseEnabled: false,
		},
		{
			Name: "frame3",
			Fields: []pdk.FieldSpec{
				{
					Name: "field1",
					Min:  0,
					Max:  3999999,
				},
				{
					Name: "field2",
					Min:  10000,
					Max:  20000,
				},
			},
		},
	}

	_, err := pdk.SetupPilosa([]string{host}, "newindex", frames, 2)
	if err != nil {
		t.Fatalf("SetupPilosa: %v", err)
	}

	client, err := pcli.NewClientFromAddresses([]string{host}, nil)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	for key, idx := range schema.Indexes() {
		fmt.Printf("%v, %#v\n", key, idx)
	}

}

func MustNewRunningServer(t *testing.T) *server.Command {
	s := server.NewCommand(&bytes.Buffer{}, ioutil.Discard, ioutil.Discard)
	s.Config.Bind = ":0"
	port := strconv.Itoa(MustOpenPort(t))
	s.Config.GossipPort = port
	s.Config.GossipSeed = "localhost:" + port
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("error creating temp data directory: %v", err)
	}
	s.Config.DataDir = td
	err = s.Run()
	if err != nil {
		t.Fatalf("error running new pilosa server: %v", err)
	}
	return s
}

func MustOpenPort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		t.Fatalf("resolving new port addr: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("listening to get new port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
