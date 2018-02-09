package taxi_test

import (
	"fmt"
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk/usecase/taxi"
	"github.com/pilosa/pilosa/test"
)

func TestRunMain(t *testing.T) {
	// start up pilosa cluster
	cluster := test.MustRunMainWithCluster(t, 3)
	for i, s := range cluster {
		fmt.Println("coordinator", i, s.Server.Cluster.Coordinator)
	}
	client, err := gopilosa.NewClient(cluster[0].Server.Addr().String())
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}

	// make sure all nodes have joined cluster.
	var status gopilosa.Status
	for {
		status, err = client.Status()
		if err != nil {
			t.Fatalf("getting status: %v", err)
		}
		if len(status.Nodes) == 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	client, err = gopilosa.NewClient([]string{cluster[0].Server.Addr().String(), cluster[1].Server.Addr().String(), cluster[2].Server.Addr().String()})
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}

	// run taxi import with testdata
	main := taxi.NewMain()
	main.URLFile = "testdata/urls.txt"
	main.Index = "taxi"
	main.Concurrency = 2
	main.FetchConcurrency = 3
	main.PilosaHost = cluster[0].Server.Addr().String()
	fmt.Println("pilosa", main.PilosaHost)
	main.BufferSize = 100000
	err = main.Run()
	if err != nil {
		t.Fatalf("running taxi main: %v", err)
	}

	// query pilosa to ensure consistent results
	index, cabTypeFrame := GetFrame(t, client, "taxi", "cab_type")

	resp, err := client.Query(index.Count(cabTypeFrame.Bitmap(0)))
	if err != nil {
		t.Fatalf("count querying: %v", err)
	}
	if resp.Result().Count() != 34221 {
		t.Fatalf("cab_type 0 should have 34221, but got %d", resp.Result().Count())
	}

	resp, err = client.Query(index.Count(cabTypeFrame.Bitmap(1)))
	if err != nil {
		t.Fatalf("count querying: %v", err)
	}
	if resp.Result().Count() != 87793 {
		t.Fatalf("cab_type 0 should have 87793, but got %d", resp.Result().Count())
	}

	resp, err = client.Query(cabTypeFrame.TopN(5))
	if err != nil {
		t.Fatalf("topn query: %v", err)
	}
	items := resp.Result().CountItems()
	if len(items) != 2 {
		t.Fatalf("wrong number of results for Topn(cab_type): %v", items)
	}
	if items[0].ID != 1 || items[0].Count != 87793 {
		t.Fatalf("wrong first item for Topn(cab_type): %v", items)
	}

	if items[1].ID != 0 || items[1].Count != 34221 {
		t.Fatalf("wrong second item for Topn(cab_type): %v", items)
	}

}

func GetFrame(t *testing.T, c *gopilosa.Client, index, frame string) (*gopilosa.Index, *gopilosa.Frame) {
	schema, err := c.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	idx, err := schema.Index(index)
	if err != nil {
		t.Fatalf("getting index: %v", err)
	}

	fram, err := idx.Frame(frame)
	if err != nil {
		t.Fatalf("getting frame: %v", err)
	}
	err = c.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	return idx, fram
}
