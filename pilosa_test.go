package pdk_test

import (
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	ptest "github.com/pilosa/pilosa/test"
)

func TestSetupPilosa(t *testing.T) {
	s := ptest.MustRunMainWithCluster(t, 2)
	hosts := []string{}
	for _, com := range s {
		hosts = append(hosts, "http://"+com.Server.Addr().String())
	}

	schema := gopilosa.NewSchema()
	index, err := schema.Index("newindex")
	if err != nil {
		t.Fatal(err)
	}
	index.Field("field1", gopilosa.OptFieldSet(gopilosa.CacheTypeRanked, 17))
	index.Field("field2", gopilosa.OptFieldSet(gopilosa.CacheTypeLRU, 19))
	index.Field("field3", gopilosa.OptFieldInt(0, 20000))
	index.Field("fieldtime", gopilosa.OptFieldTime(gopilosa.TimeQuantumYearMonthDay))

	indexer, err := pdk.SetupPilosa(hosts, index.Name(), schema, 2)
	if err != nil {
		t.Fatalf("SetupPilosa: %v", err)
	}

	indexer.AddColumn("field1", 0, 0)
	indexer.AddValue("field3", 0, 97)
	indexer.AddColumnTimestamp("fieldtime", 0, 0, time.Date(2018, time.February, 22, 9, 0, 0, 0, time.UTC))
	indexer.AddColumnTimestamp("fieldtime", 2, 0, time.Date(2018, time.February, 24, 9, 0, 0, 0, time.UTC))
	indexer.AddValue("field3", 0, 100)

	err = indexer.Close()
	if err != nil {
		t.Fatalf("closing indexer: %v", err)
	}

	client, err := gopilosa.NewClient(hosts)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	schema, err = client.Schema()
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

	if len(idxs["newindex"].Fields()) != 4 {
		t.Fatalf("wrong number of fields: %v", idxs["newindex"].Fields())
	}

	idx, err := schema.Index("newindex")
	if err != nil {
		t.Fatalf("getting index: %v", err)
	}
	fieldtime, err := idx.Field("fieldtime")
	if err != nil {
		t.Fatalf("getting field: %v", err)
	}
	resp, err := client.Query(fieldtime.Range(0, time.Date(2018, time.February, 21, 9, 0, 0, 0, time.UTC), time.Date(2018, time.February, 23, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits := resp.Result().Row().Columns
	if len(bits) != 1 || bits[0] != 0 {
		t.Fatalf("unexpected bits from range query: %v", bits)
	}

	resp, err = client.Query(fieldtime.Range(0, time.Date(2018, time.February, 20, 9, 0, 0, 0, time.UTC), time.Date(2018, time.February, 21, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits = resp.Result().Row().Columns
	if len(bits) != 0 {
		t.Fatalf("unexpected bits from empty range query: %v", bits)
	}

	resp, err = client.Query(fieldtime.Range(0, time.Date(2018, time.February, 20, 9, 0, 0, 0, time.UTC), time.Date(2018, time.February, 25, 9, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits = resp.Result().Row().Columns
	if len(bits) != 2 || bits[1] != 2 || bits[0] != 0 {
		t.Fatalf("unexpected bits from empty range query: %v", bits)
	}

	field3, err := idx.Field("field3")
	if err != nil {
		t.Fatalf("getting field: %v", err)
	}

	resp, err = client.Query(field3.Equals(100))
	if err != nil {
		t.Fatalf("executing range query: %v", err)
	}
	bits = resp.Result().Row().Columns
	if len(bits) != 1 || bits[0] != 0 {
		t.Fatalf("unexpected bits from range field query: %v", bits)
	}

}
