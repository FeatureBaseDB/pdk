package file

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

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

	// http.Post("http://"+pilosaHost+"/recalculate-caches", "", strings.NewReader(""))

	// fmt.Printf("%s\n", mustQueryHost(t, "Bitmap(frame=stuff, row=0)", pilosaHost))
	// fmt.Printf("%s\n", mustQueryHost(t, "TopN(frame=stuff)", pilosaHost))

	// fmt.Printf("%s\n", mustQuery(t, "Bitmap(frame=stuff, row=stuff1)"))
	// fmt.Printf("%s\n", mustQuery(t, "TopN(frame=stuff)"))

}

var data = `
{"id": "123", "value": 17}
{"id": "120", "value": 16}
{"id": "119", "value": 19}
`

func TestMinimalBreak(t *testing.T) {
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

func check(t *testing.T, err error, r *http.Response) {
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode > 300 {
		bod, _ := ioutil.ReadAll(r.Body)
		t.Logf("status fail %d %s", r.StatusCode, bod)
		panic("blah")
	}
}

func TestSimpleBreak(t *testing.T) {
	pilosa := test.MustRunMainWithCluster(t, 1)
	pilosaHost := pilosa[0].Server.Addr().String()
	r, err := http.Get("http://" + pilosaHost + "/schema")
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/index/pdk", "", bytes.NewBuffer([]byte{}))
	check(t, err, r)
	r, err = http.Get("http://" + pilosaHost + "/schema")
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/index/pdk/frame/stuff", "application/x-protobuf", bytes.NewBuffer([]byte{0x7b, 0x22, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0x3a, 0x20, 0x7b, 0x22, 0x63, 0x61, 0x63, 0x68, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x3a, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x2c, 0x22, 0x63, 0x61, 0x63, 0x68, 0x65, 0x54, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x72, 0x61, 0x6e, 0x6b, 0x65, 0x64, 0x22, 0x7d, 0x7d}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/index/pdk/frame/default", "application/x-protobuf", bytes.NewBuffer([]byte{0x7b, 0x22, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0x3a, 0x20, 0x7b, 0x22, 0x63, 0x61, 0x63, 0x68, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x3a, 0x31, 0x30, 0x30, 0x30, 0x2c, 0x22, 0x63, 0x61, 0x63, 0x68, 0x65, 0x54, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x72, 0x61, 0x6e, 0x6b, 0x65, 0x64, 0x22, 0x7d, 0x7d}))
	check(t, err, r)
	r, err = http.Get("http://" + pilosaHost + "/fragment/nodes?slice=0&index=pdk")
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/index/pdk/frame/default/field/value", "application/x-protobuf", bytes.NewBuffer([]byte{0x7b, 0x22, 0x6d, 0x61, 0x78, 0x22, 0x3a, 0x32, 0x31, 0x34, 0x37, 0x34, 0x38, 0x33, 0x36, 0x34, 0x37, 0x2c, 0x22, 0x6d, 0x69, 0x6e, 0x22, 0x3a, 0x30, 0x2c, 0x22, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x2c, 0x22, 0x74, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x69, 0x6e, 0x74, 0x22, 0x7d}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import-value", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x7, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x22, 0x5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x11}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x5, 0x73, 0x74, 0x75, 0x66, 0x66, 0x22, 0x1, 0x0, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x0}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import-value", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x7, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x22, 0x5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x10}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x5, 0x73, 0x74, 0x75, 0x66, 0x66, 0x22, 0x1, 0x0, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x0}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import-value", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x7, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x22, 0x5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x10}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x5, 0x73, 0x74, 0x75, 0x66, 0x66, 0x22, 0x1, 0x0, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x0}))
	check(t, err, r)
	r, err = http.Post("http://"+pilosaHost+"/import-value", "application/x-protobuf", bytes.NewBuffer([]byte{0xa, 0x3, 0x70, 0x64, 0x6b, 0x12, 0x7, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x22, 0x5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2a, 0x1, 0x0, 0x32, 0x1, 0x10}))
	check(t, err, r)

	time.Sleep(time.Second)

	http.Post("http://"+pilosaHost+"/recalculate-caches", "", strings.NewReader(""))

	fmt.Printf("%s\n", mustQueryHost(t, "Bitmap(frame=stuff, row=0)", pilosaHost))
	fmt.Printf("%s\n", mustQueryHost(t, "TopN(frame=stuff)", pilosaHost))

}
