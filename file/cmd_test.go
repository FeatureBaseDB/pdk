package file

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa/test"
	"github.com/pkg/errors"
)

func TestFileIngest(t *testing.T) {
	pilosa := test.MustRunMainWithCluster(t, 1)

	fname := newFileWithData(t, data)
	cmd := NewMain()
	cmd.Path = fname
	pilosaHost := pilosa[0].Server.Addr().String()
	cmd.PilosaHosts = []string{pilosaHost}
	cmd.BatchSize = 1
	cmd.SubjectPath = []string{"id"}
	cmd.SubjectAt = ""
	cmd.Proxy = "localhost:55346"
	if err := cmd.Run(); err != nil {
		t.Fatalf("running ingester: %v", err)
	}

	http.Post("http://"+pilosaHost+"/recalculate-caches", "", strings.NewReader(""))

	fmt.Printf("%s", mustQueryHost(t, "Bitmap(frame=stuff, row=0)", pilosaHost))
	fmt.Printf("%s", mustQueryHost(t, "TopN(frame=stuff)", pilosaHost))

	fmt.Printf("%s", mustQuery(t, "Bitmap(frame=stuff, row=stuff1)"))
	fmt.Printf("%s", mustQuery(t, "TopN(frame=stuff)"))

}

var data = `{"id": "123", "value": 17, "stuff": "stuff1"}
{"id": "122", "value": 16, "stuff": "stuff2"}
{"id": "121", "value": 16, "stuff": "stuff3"}
{"id": "120", "value": 16, "stuff": "stuff2"}
{"id": "119", "value": 19, "stuff": "stuff1"}
{"id": "123", "value": 22, "stuff": "stuff2"}`

func TestMinimalBreak(t *testing.T) {
	t.SkipNow()
	pilosa := test.MustRunMainWithCluster(t, 1)
	pilosaHost := pilosa[0].Server.Addr().String()
	indexer, err := pdk.SetupPilosa([]string{pilosaHost}, "pdk", []pdk.FrameSpec{}, 1)
	if err != nil {
		t.Fatal(errors.Wrap(err, "setting up Pilosa"))
	}
	indexer.AddValue("default", "value", 0, 17)
	indexer.AddValue("default", "value", 0, 16)
	indexer.AddValue("default", "value", 0, 19)
	err = indexer.Close()
	if err != nil {
		t.Fatalf("closing indexer: %v", err)
	}
}

func mustQuery(t *testing.T, q string) string {
	resp, err := http.Post("http://localhost:55346/index/pdk/query", "application/pql", strings.NewReader(q))

	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	bod, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(bod)

}
func mustQueryHost(t *testing.T, q string, host string) string {
	resp, err := http.Post("http://"+host+"/index/pdk/query", "application/pql", strings.NewReader(q))

	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	bod, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(bod)
}

func newFileWithData(t *testing.T, data string) string {
	tf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer tf.Close()

	_, err = tf.WriteString(data)
	if err != nil {
		t.Fatal(err)
	}

	return tf.Name()
}
