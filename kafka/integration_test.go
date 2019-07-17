// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package kafka_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk/kafka"
	datagen "github.com/pilosa/pdk/kafka/datagen"
	"github.com/pilosa/pilosa/test"
)

var kafkaGroup = "testgroup"

func TestSource(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	for i := 0; i < 10; i++ {
		_, err := datagen.PostData()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	src := kafka.NewConfluentSource()
	src.Hosts = []string{"localhost:9092"}
	src.Group = kafkaGroup
	src.Topics = []string{datagen.KafkaTopic}
	src.RegistryURL = "localhost:8081"
	src.Type = "raw"
	err := src.Open()
	if err != nil {
		t.Fatalf("opening kafka source: %v", err)
	}

	rec, err := src.Record()
	if err != nil {
		t.Fatalf("getting record: %v", err)
	}
	recmap, ok := rec.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected record %v of type %[1]T", rec)
	}

	keys := []string{"customer_id", "geoip", "aba", "db", "user_id", "timestamp"}
	for _, k := range keys {
		if _, ok := recmap[k]; !ok {
			t.Fatalf("key %v not found in record", k)
		}
	}
	geokeys := []string{"time_zone", "longitude", "latitude", "country_name", "dma_code", "city", "region", "metro_code", "postal_code", "area_code", "region_name", "country_code3", "country_code"}
	for _, k := range geokeys {
		if _, ok := recmap["geoip"].(map[string]interface{})[k]; !ok {
			t.Fatalf("key %v not found in record", k)
		}
	}

}

func compareStringLists(act, exp []string) (unexpected, unfound []string) {
	sort.Strings(act)
	sort.Strings(exp)
	i1, i2 := 0, 0
	for i1 < len(act) && i2 < len(exp) {
		v1, v2 := act[i1], exp[i2]
		if v1 == v2 {
			i1++
			i2++
		} else if v1 < v2 {
			unexpected = append(unexpected, v1)
			i1++
		} else if v2 < v1 {
			unfound = append(unfound, v2)
			i2++
		}
	}
	if i1 < len(act) {
		unexpected = append(unexpected, act[i1:]...)
	}
	if i2 < len(exp) {
		unfound = append(unfound, exp[i2:]...)
	}
	return unexpected, unfound
}

func TestCompareStringLists(t *testing.T) {
	tests := []struct {
		act []string
		exp []string
		une []string
		unf []string
	}{
		{
			act: []string{},
			exp: []string{},
			une: nil,
			unf: nil},
		{
			act: []string{"a", "b"},
			exp: []string{"a", "b"},
			une: nil,
			unf: nil},
		{
			act: []string{"a", "b", "c"},
			exp: []string{"a", "b"},
			une: []string{"c"},
			unf: nil},
		{
			act: []string{"a", "b", "c"},
			exp: []string{"a", "b", "d"},
			une: []string{"c"},
			unf: []string{"d"}},
		{
			act: []string{"c", "b", "a"},
			exp: []string{"a", "b", "c"},
			une: nil,
			unf: nil},
		{
			act: []string{"", "z", "c", "b", "a"},
			exp: []string{"a", "b", "c"},
			une: []string{"", "z"},
			unf: nil},
		{
			act: []string{"a", "b", "c"},
			exp: []string{"", "z", "c", "b", "a"},
			une: nil,
			unf: []string{"", "z"}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			une, unf := compareStringLists(test.act, test.exp)
			if !reflect.DeepEqual(unf, test.unf) {
				t.Errorf("Expected unfound:\n%#v\nGot unfound:\n%#v", test.unf, unf)
			}
			if !reflect.DeepEqual(une, test.une) {
				t.Errorf("Expected unexpected:\n%#v\nGot unexpected:\n%#v", test.une, une)
			}
		})
	}

}

// TestMain relies on having a running instance of kafka, schema-registry,
// and rest proxy running. Currently using confluent-3.3.0 which you can get
// here: https://www.confluent.io/download Decompress, enter directory, then run
// "./bin/confluent start kafka-rest"
func TestMain(t *testing.T) {
	runMain(t, []string{})
	// without this sleep, the next test will hang sometimes. I'm guessing
	// something in Confluent or the OS needs to settle between tests.
	time.Sleep(time.Second)
}

func TestAllowedFields(t *testing.T) {
	runMain(t, []string{"geoip-country_code", "aba"})
	time.Sleep(time.Second)
}

func runMain(t *testing.T, allowedFields []string) {
	if testing.Short() {
		t.Skip("integration test")
	}
	for i := 0; i < 1000; i++ {
		_, err := datagen.PostData()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	m := kafka.NewMain()
	pilosa := test.MustRunCluster(t, 1)
	defer func() {
		err := m.Close()
		if err != nil {
			t.Logf("closing kafka ingest main: %v", err)
		}
		err = pilosa.Close()
		if err != nil {
			t.Logf("closing cluster: %v", err)
		}
	}()
	pilosaHost := pilosa[0].API.Node().URI.HostPort()
	m.PilosaHosts = []string{pilosaHost}
	m.BatchSize = 300
	m.AllowedFields = allowedFields
	m.SubjectPath = []string{"user_id"}
	m.Topics = []string{datagen.KafkaTopic}
	m.Proxy = ":39485"
	m.MaxRecords = 1000
	var err error
	m.TranslatorDir, err = ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("making temp dir for translation: %v", err)
	}
	defer func() {
		os.RemoveAll(m.TranslatorDir)
	}()

	err = m.Run()
	if err != nil {
		t.Fatalf("error running: %v", err)
	}

	_, err = http.Post("http://"+pilosaHost+"/recalculate-caches", "", strings.NewReader(""))
	if err != nil {
		t.Fatalf("recalcing caches: %v", err)
	}

	cli, err := gopilosa.NewClient([]string{pilosaHost})
	if err != nil {
		t.Fatalf("getting pilosa client: %v", err)
	}

	schema, err := cli.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	idx := schema.Index("pdk")

	fieldlist := []string{}
	for name, field := range idx.Fields() {
		fieldlist = append(fieldlist, name)
		if field.Options().Type() == gopilosa.FieldTypeInt {
			if resp, err := cli.Query(field.Sum(field.GTE(0))); err != nil {
				t.Errorf("query for field (%v): %v", name, err)
			} else {
				fmt.Printf("%v, Sum: %v\n", name, resp.Result().Value())
			}
		} else if field.Options().Type() == gopilosa.FieldTypeSet {
			if resp, err := cli.Query(field.TopN(10)); err != nil {
				t.Errorf("field topn query (%v): %v", name, err)
			} else {
				fmt.Printf("%v: TopN: %v\n", name, resp.Result().CountItems())
			}
		} else if field.Options().Type() == gopilosa.FieldTypeTime {
			if resp, err := cli.Query(field.Range(0, time.Unix(0, 0), time.Unix(1931228574, 0))); err != nil {
				t.Errorf("field range query (%v): %v", name, err)
			} else {
				fmt.Printf("%v: Range: %v\n", name, resp.Result())
			}
		}
	}

	resp := mustQuery(t, "TopN(geoip-country_code, n=100)")
	if !strings.Contains(resp, `","Count":2},{"Key":"`) {
		t.Fatalf("unexpected result from TopN(geoip-country_code, n=10):\n%v", resp)
	}

	expFields := []string{"geoip-region", "geoip-city", "geoip-country_name", "timestamp", "aba", "geoip-country_code", "geoip-region_name", "db", "geoip-country_code3", "geoip-postal_code", "geoip-time_zone", "customer_id", "geoip-area_code", "geoip-dma_code", "geoip-latitude", "geoip-longitude", "geoip-metro_code"}
	if len(allowedFields) > 0 {
		expFields = allowedFields
	}
	if unexp, unfound := compareStringLists(fieldlist, expFields); len(unexp) > 0 || len(unfound) > 0 {
		t.Errorf("got unexpected fields:%v", unexp)
		t.Errorf("didn't find fields:   %v", unfound)
	}

	fmt.Println(mustHTTP(t, pilosaHost, "/schema"))
}

func mustQuery(t *testing.T, q string) string {
	resp, err := http.Post("http://localhost:39485/index/pdk/query", "application/pql", strings.NewReader(q))

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

func mustHTTP(t *testing.T, host, path string) string {
	resp, err := http.Get("http://" + host + path)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode > 299 {
		t.Fatal("bad status")
	}
	bod, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(bod)
}
