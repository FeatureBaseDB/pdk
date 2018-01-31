package pdk_test

import (
	"testing"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	ptest "github.com/pilosa/pilosa/test"
)

func TestSetupPilosa(t *testing.T) {
	s := ptest.MustNewRunningServer(t)
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

	idxs := schema.Indexes()
	if len(idxs) != 1 {
		t.Fatalf("too many indexes: %v", idxs)
	}
	if idx, ok := idxs["newindex"]; !ok {
		t.Fatalf("index with wrong name: %v", idx)
	}

	if len(idxs["newindex"].Frames()) != 3 {
		t.Fatalf("wrong number of frames: %v", idxs["newindex"].Frames())
	}

}
