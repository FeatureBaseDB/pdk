package fakeusers_test

import (
	"testing"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pilosa/test"

	"github.com/pilosa/pdk/usecase/fakeusers"
)

func TestFakeUsers(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	client, err := gopilosa.NewClient([]string{cluster[0].URL(), cluster[1].URL(), cluster[2].URL()})
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}

	main := fakeusers.NewMain()

	main.Num = 1000
	main.PilosaHosts = []string{cluster[0].URL(), cluster[1].URL(), cluster[2].URL()}

	main.Run()

	_, titleField := GetField(t, client, "users", "title")

	resp := query(t, client, titleField.TopN(10))
	items := resp.Result().CountItems()
	if len(items[0].Key) < 1 {
		t.Fatalf("should have a key: %#v", items[0])
	}
	if items[0].Count < items[1].Count || items[1].Count < 1 {
		t.Fatalf("bad counts: %v", items)
	}

}

func query(t *testing.T, c *gopilosa.Client, query gopilosa.PQLQuery) *gopilosa.QueryResponse {
	resp, err := c.Query(query)
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	return resp
}

func GetField(t *testing.T, c *gopilosa.Client, index, field string) (*gopilosa.Index, *gopilosa.Field) {
	schema, err := c.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	idx := schema.Index(index)
	fram := idx.Field(field)
	err = c.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	return idx, fram
}
