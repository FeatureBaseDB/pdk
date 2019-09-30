package csrc_test

import (
	"testing"

	"github.com/pilosa/pdk/v2/kafka/csrc"
)

func TestPostGet(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	sr := "localhost:8081"
	client := csrc.NewClient(sr)

	schemaStr := `{"type":"record","name":"a","fields":[{"name":"blah","type":"string"}]}`
	r, err := client.PostSubjects("aname", schemaStr)
	if err != nil {
		t.Fatalf("postsubjects: %v", err)
	}

	// Docs indicate that schema and subject should be returned by the
	// POST, but they are not.
	//
	// if r.Schema != schemaStr {
	// 	t.Errorf("wrong schema: %s", r.Schema)
	// }

	// if r.Subject != "aname" {
	// 	t.Errorf("wrong name: %v", r.Subject)
	// }

	sch, err := client.GetSchema(r.ID)
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	if sch != schemaStr {
		t.Errorf("unexpected schema\n%s\n%s", sch, schemaStr)
	}
}
