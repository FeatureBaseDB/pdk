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

// +build integration

package kafka_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/kafka"
	"github.com/pilosa/pilosa/test"
)

var kafkaTopic = "testtopic"
var kafkaGroup = "testgroup"

func TestSource(t *testing.T) {
	for i := 0; i < 10; i++ {
		postData(t)
	}

	src := kafka.NewConfluentSource()
	src.Hosts = []string{"localhost:9092"}
	src.Group = kafkaGroup
	src.Topics = []string{kafkaTopic}
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

// TestEverything relies on having a running instance of kafka, schema-registry,
// and rest proxy running. Currently using confluent-3.3.0 which you can get
// here: https://www.confluent.io/download Decompress, enter directory, then run
// "./bin/confluent start kafka-rest"
func TestEverything(t *testing.T) {
	runEverything(t)
}

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func runEverything(t *testing.T) {
	for i := 0; i < 1000; i++ {
		postData(t)
	}

	src := kafka.NewConfluentSource()
	src.Hosts = []string{"localhost:9092"}
	src.Group = kafkaGroup
	src.Topics = []string{kafkaTopic}
	src.RegistryURL = "localhost:8081"
	err := src.Open()
	if err != nil {
		t.Fatalf("opening kafka source: %v", err)
	}

	parser := pdk.NewDefaultGenericParser()
	mapper := pdk.NewCollapsingMapper()

	mains := test.MustRunCluster(t, 3)
	defer func() {
		err := mains.Close()
		if err != nil {
			t.Logf("closing cluster: %v", err)
		}
	}()
	var hosts []string
	for _, m := range mains {
		hosts = append(hosts, m.URL())
	}
	schema := gopilosa.NewSchema()
	idx, err := schema.Index("kafkaavro")
	check(t, err)
	_, err = idx.Field("geoip-latitude", gopilosa.OptFieldInt(-90, 90))
	check(t, err)
	_, err = idx.Field("geoip-longitude", gopilosa.OptFieldInt(-180, 180))
	check(t, err)
	idxer, err := pdk.SetupPilosa([]string{hosts[0]}, "kafkaavro", schema, 10)
	if err != nil {
		t.Fatalf("setting up pilosa: %v", err)
	}
	done := make(chan struct{})
	go func() {
		time.Sleep(time.Second * 2)
		err = src.Close()
		if err != nil {
			t.Logf("closing kafka source: %v", err)
		}
		close(done)
	}()

	ingester := pdk.NewIngester(src, parser, mapper, idxer)
	err = ingester.Run()
	if err != nil {
		t.Fatalf("running ingester: %v", err)
	}
	<-done

	cli, err := gopilosa.NewClient([]string{hosts[0]})
	if err != nil {
		t.Fatalf("getting pilosa client: %v", err)
	}

	schema, err = cli.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	idx, err = schema.Index("kafkaavro")
	if err != nil {
		t.Fatalf("getting index: %v", err)
	}

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

	expFields := []string{"geoip-region", "geoip-city", "geoip-country_name", "timestamp", "aba", "geoip-country_code", "geoip-region_name", "db", "geoip-country_code3", "geoip-postal_code", "geoip-time_zone", "customer_id", "geoip-area_code", "geoip-dma_code", "geoip-latitude", "geoip-longitude", "geoip-metro_code", "user_id"}
	if unexp, unfound := compareStringLists(fieldlist, expFields); len(unexp) > 0 || len(unfound) > 0 {
		t.Errorf("got unexpected fields:%v", unexp)
		t.Errorf("didn't find fields:   %v", unfound)
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
}

func TestAllowedFields(t *testing.T) {
	runMain(t, []string{"geoip-country_code", "aba"})
}

func runMain(t *testing.T, allowedFields []string) {
	for i := 0; i < 1000; i++ {
		postData(t)
	}

	m := kafka.NewMain()
	pilosa := test.MustRunCluster(t, 1)
	defer func() {
		err := pilosa.Close()
		if err != nil {
			t.Logf("closing cluster: %v", err)
		}
	}()
	pilosaHost := pilosa[0].API.Node().URI.HostPort()
	m.PilosaHosts = []string{pilosaHost}
	m.BatchSize = 300
	m.AllowedFields = allowedFields
	m.SubjectPath = []string{"user_id"}
	m.Topics = []string{kafkaTopic}
	m.Proxy = ":39485"
	m.MaxRecords = 1000

	err := m.Run()
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

	idx, err := schema.Index("pdk")
	if err != nil {
		t.Fatalf("getting index: %v", err)
	}

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

func postData(t *testing.T) (response map[string]interface{}) {
	krpURL := "localhost:8082"
	postURL := fmt.Sprintf("http://%s/topics/%s", krpURL, kafkaTopic)
	data := struct {
		Schema  string  `json:"value_schema"`
		Records []Value `json:"records"`
	}{
		Schema:  schema,
		Records: []Value{{Value: map[string]interface{}{"com.pi.Stuff": GenRecord()}}},
	}
	dataBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Fatalf("marshalling schema: %v", err)
	}

	resp, err := http.Post(postURL, "application/vnd.kafka.avro.v2+json", bytes.NewBuffer(dataBytes))
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}

	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		t.Fatalf("unexpected status posting data: %v, body: %s", resp.StatusCode, respBody)
	}

	respMap := make(map[string]interface{})
	err = json.Unmarshal(respBody, &respMap)
	if err != nil {
		t.Fatalf("decoding post data response: %v", err)
	}
	return respMap
}

type Value struct {
	Value map[string]interface{} `json:"value"`
}

func GenRecord() *Record {
	geo := GeoIP{
		TimeZone:     TimeZone(),
		Longitude:    Longitude(),
		Latitude:     Latitude(),
		CountryName:  CountryName(),
		DmaCode:      DmaCode(),
		City:         City(),
		Region:       Region(),
		MetroCode:    MetroCode(),
		PostalCode:   PostalCode(),
		AreaCode:     AreaCode(),
		RegionName:   RegionName(),
		CountryCode3: CountryCode3(),
		CountryCode:  CountryCode(),
	}
	return &Record{
		ABA:        ABA(),
		Db:         Db(),
		UserID:     UserID(),
		CustomerID: CustomerID(),
		GeoIP:      geo,
	}
}

func Db() string {
	return text(1, 6, true, true, true, true)
}

func UserID() int {
	return rand.Intn(10000000) // 10 mil
}

func CustomerID() int {
	return rand.Intn(1000000) // 1 mil
}

func TimeZone() string {
	idx := rand.Intn(len(tzs))
	return tzs[idx]
}

func Longitude() float64 {
	ran := rand.ExpFloat64() * 10.0
	for ran > 360.0 {
		ran = rand.ExpFloat64() * 10.0
	}
	return ran - 180.0
}

func Latitude() float64 {
	ran := rand.ExpFloat64() * 10.0
	for ran > 180.0 {
		ran = rand.ExpFloat64() * 5.0
	}
	return ran - 90.0
}

func CountryName() string {
	base := text(1, 3, true, false, false, false)
	return base + base
}

func DmaCode() int {
	return rand.Intn(100000)
}

func City() string {
	base := text(1, 4, true, false, false, false)
	return base + base
}

func Region() string {
	base := text(1, 3, false, true, false, false)
	return base
}

func MetroCode() int {
	return rand.Intn(10000)
}

func PostalCode() string {
	return strconv.Itoa(rand.Intn(98999) + 10000)
}

func AreaCode() int {
	return rand.Intn(899) + 100
}

func RegionName() string {
	return ""
}

func CountryCode3() string {
	return ""
}

func CountryCode() string {
	return text(3, 3, false, true, false, false)
}

// ABA returns a random 9 numeric digit string with about 27000 possible values.
func ABA() string {
	num := rand.Intn(27000) + 22213
	num2 := num/10 - 1213
	numstr := strconv.Itoa(num)
	num2str := strconv.Itoa(num2)
	numstrbytes := append([]byte(numstr), num2str[3], numstr[0], numstr[1], numstr[2])
	return string(numstrbytes)
}

var lowerLetters = []rune("abcdefghijklmnopqrstuvwxyz")
var upperLetters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var numeric = []rune("0123456789")
var specialChars = []rune(`!'@#$%^&*()_+-=[]{};:",./?`)
var hexDigits = []rune("0123456789abcdef")

func text(atLeast, atMost int, allowLower, allowUpper, allowNumeric, allowSpecial bool) string {
	allowedChars := []rune{}
	if allowLower {
		allowedChars = append(allowedChars, lowerLetters...)
	}
	if allowUpper {
		allowedChars = append(allowedChars, upperLetters...)
	}
	if allowNumeric {
		allowedChars = append(allowedChars, numeric...)
	}
	if allowSpecial {
		allowedChars = append(allowedChars, specialChars...)
	}

	result := []rune{}
	nTimes := rand.Intn(atMost-atLeast+1) + atLeast
	for i := 0; i < nTimes; i++ {
		result = append(result, allowedChars[rand.Intn(len(allowedChars))])
	}
	return string(result)
}

// DigitsN returns n digits as a string
func DigitsN(n int) string {
	digits := make([]rune, n)
	for i := 0; i < n; i++ {
		digits[i] = numeric[rand.Intn(len(numeric))]
	}
	return string(digits)
}

// Digits returns from 1 to 5 digits as a string
func Digits() string {
	return DigitsN(rand.Intn(5) + 1)
}

func hexDigitsStr(n int) string {
	var num []rune
	for i := 0; i < n; i++ {
		num = append(num, hexDigits[rand.Intn(len(hexDigits))])
	}
	return string(num)
}

// HexColor generates hex color name
func HexColor() string {
	return hexDigitsStr(6)
}

// HexColorShort generates short hex color name
func HexColorShort() string {
	return hexDigitsStr(3)
}

var tzs = []string{
	"ACDT", "ACST", "ACT", "ACT", "ACWST", "ADT", "AEDT", "AEST", "AFT", "AKDT",
	"AKST", "AMST", "AMT", "AMT", "ART", "AST", "AST", "AWST", "AZOST", "AZOT",
	"AZT", "BDT", "BIOT", "BIT", "BOT", "BRST", "BRT", "BST", "BST", "BST",
	"BTT", "CAT", "CCT", "CDT", "CDT", "CEST", "CET", "CHADT", "CHAST", "CHOT",
	"CHOST", "CHST", "CHUT", "CIST", "CIT", "CKT", "CLST", "CLT", "COST", "COT",
	"CST", "CST", "ACST", "ACDT", "CST", "CT", "CVT", "CWST", "CXT", "DAVT",
	"DDUT", "DFT", "EASST", "EAST", "EAT", "ECT", "ECT", "EDT", "AEDT", "EEST",
	"EET", "EGST", "EGT", "EIT", "EST", "AEST", "FET", "FJT", "FKST", "FKT",
	"FNT", "GALT", "GAMT", "GET", "GFT", "GILT", "GIT", "GMT", "GST", "GST",
	"GYT", "HDT", "HAEC", "HST", "HKT", "HMT", "HOVST", "HOVT", "ICT", "IDT",
	"IOT", "IRDT", "IRKT", "IRST", "IST", "IST", "IST", "JST", "KGT", "KOST",
	"KRAT", "KST", "LHST", "LHST", "LINT", "MAGT", "MART", "MAWT", "MDT", "MET",
	"MEST", "MHT", "MIST", "MIT", "MMT", "MSK", "MST", "MST", "MUT", "MVT",
	"MYT", "NCT", "NDT", "NFT", "NPT", "NST", "NT", "NUT", "NZDT", "NZST",
	"OMST", "ORAT", "PDT", "PET", "PETT", "PGT", "PHOT", "PHT", "PKT", "PMDT",
	"PMST", "PONT", "PST", "PST", "PYST", "PYT", "RET", "ROTT", "SAKT", "SAMT",
	"SAST", "SBT", "SCT", "SDT", "SGT", "SLST", "SRET", "SRT", "SST", "SST",
	"SYOT", "TAHT", "THA", "TFT", "TJT", "TKT", "TLT", "TMT", "TRT", "TOT",
	"TVT", "ULAST", "ULAT", "USZ1", "UTC", "UYST", "UYT", "UZT", "VET", "VLAT",
	"VOLT", "VOST", "VUT", "WAKT", "WAST", "WAT", "WEST", "WET", "WIT", "WST",
	"YAKT", "YEKT"}

type Record struct {
	ABA        string `json:"aba"`
	Db         string `json:"db"`
	UserID     int    `json:"user_id"`
	CustomerID int    `json:"customer_id"`
	Timestamp  string `json:"timestamp"`
	GeoIP      GeoIP  `json:"geoip"`
}

type GeoIP struct {
	TimeZone     string  `json:"time_zone"`
	Longitude    float64 `json:"longitude"`
	Latitude     float64 `json:"latitude"`
	CountryName  string  `json:"country_name"`
	DmaCode      int     `json:"dma_code"`
	City         string  `json:"city"`
	Region       string  `json:"region"`
	MetroCode    int     `json:"metro_code"`
	PostalCode   string  `json:"postal_code"`
	AreaCode     int     `json:"area_code"`
	RegionName   string  `json:"region_name"`
	CountryCode3 string  `json:"country_code3"`
	CountryCode  string  `json:"country_code"`
}

type stringThing struct {
	String string `json:"string"`
}

type doubleThing struct {
	Double float64 `json:"double"`
}

type intThing struct {
	Int int `json:"int"`
}

// avroJSONGeoIP exists because the JSON to avro converter in Confluent requires
// a somewhat wacky json encoding in order to differentiate between types in
// unions in the case where json would be ambiguous.
// https://stackoverflow.com/a/27499930/2088767
type avroJSONGeoIP struct {
	GeoIP ajgip `json:"com.pi.GeoIPResult"`
}

type ajgip struct {
	TimeZone     stringThing `json:"time_zone"`
	Longitude    doubleThing `json:"longitude"`
	Latitude     doubleThing `json:"latitude"`
	CountryName  stringThing `json:"country_name"`
	DmaCode      intThing    `json:"dma_code"`
	City         stringThing `json:"city"`
	Region       stringThing `json:"region"`
	MetroCode    intThing    `json:"metro_code"`
	PostalCode   stringThing `json:"postal_code"`
	AreaCode     intThing    `json:"area_code"`
	RegionName   stringThing `json:"region_name"`
	CountryCode3 stringThing `json:"country_code3"`
	CountryCode  stringThing `json:"country_code"`
}

func (g GeoIP) MarshalJSON() ([]byte, error) {
	return json.Marshal(avroJSONGeoIP{
		GeoIP: ajgip{
			TimeZone:     stringThing{g.TimeZone},
			Longitude:    doubleThing{g.Longitude},
			Latitude:     doubleThing{g.Latitude},
			CountryName:  stringThing{g.CountryName},
			DmaCode:      intThing{g.DmaCode},
			City:         stringThing{g.City},
			Region:       stringThing{g.Region},
			MetroCode:    intThing{g.MetroCode},
			PostalCode:   stringThing{g.PostalCode},
			AreaCode:     intThing{g.AreaCode},
			RegionName:   stringThing{g.RegionName},
			CountryCode3: stringThing{g.CountryCode3},
			CountryCode:  stringThing{g.CountryCode},
		},
	})
}

var schema = `[{
    "fields": [
        {
            "name": "aba",
            "type": "string"
        },
        {
            "name": "db",
            "type": "string"
        },
        {
            "name": "user_id",
            "type": "int"
        },
        {
            "name": "customer_id",
            "type": "int"
        },
        {
            "name": "timestamp",
            "type": "string"
        },
        {
            "name": "geoip",
            "type": [
                "null",
                {
                    "name": "GeoIPResult",
                    "type": "record",
                    "fields": [
                       {
                            "name": "time_zone",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "longitude",
                            "type": [
                                "null",
                                "double"
                            ]
                        },
                        {
                            "name": "country_code3",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "country_name",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "dma_code",
                            "type": [
                                "null",
                                "int"
                            ]
                        },
                        {
                            "name": "city",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "region",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "country_code",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "metro_code",
                            "type": [
                                "null",
                                "int"
                            ]
                        },
                        {
                            "name": "latitude",
                            "type": [
                                "null",
                                "double"
                            ]
                        },
                        {
                            "name": "region_name",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "postal_code",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "area_code",
                            "type": [
                                "null",
                                "int"
                            ]
                        }
                    ]
                }
            ]
        }
    ],
    "name": "Stuff",
    "namespace": "com.pi",
    "type": "record"
}]`
