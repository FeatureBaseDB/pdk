package kafka

import (
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/go-avro/avro"
	pdk "github.com/pilosa/pdk/v2"
)

func TestAvroToPDKSchema(t *testing.T) {
	tests := []struct {
		schemaFile string
		exp        []pdk.Field
		expErr     string
	}{
		{
			schemaFile: "simple.json",
			exp:        []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.StringField{NameVal: "last"}},
		},
		{
			schemaFile: "decimal.json",
			exp:        []pdk.Field{pdk.DecimalField{NameVal: "somenum", Scale: 2}},
		},
		{
			schemaFile: "notarecord.json",
			expErr:     "unsupported Avro Schema type",
		},
		{
			schemaFile: "fieldisrecord.json",
			expErr:     "nested fields are not currently supported",
		},
	}

	// check that we've covered all the test schemas
	files, err := ioutil.ReadDir("./testdata/schemas")
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}
	if len(files) != len(tests) {
		t.Fatalf("have different number of schemas and tests: %d and %d", len(files), len(tests))
	}

	for _, test := range tests {
		t.Run(test.schemaFile, func(t *testing.T) {
			codec := decodeTestSchema(t, test.schemaFile)
			schema, err := avroToPDKSchema(codec)
			if err != nil && test.expErr == "" {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.expErr != "" && err == nil {
				t.Fatalf("expected error")
			}
			if test.expErr != "" && !strings.Contains(err.Error(), test.expErr) {
				t.Fatalf("error expected/got\n%s\n%v", test.expErr, err.Error())
			}
			if !reflect.DeepEqual(test.exp, schema) {
				t.Fatalf("schema exp/got\n%+v\n%+v", test.exp, schema)
			}
		})
	}
}

func decodeTestSchema(t *testing.T, filename string) avro.Schema {
	bytes, err := ioutil.ReadFile("./testdata/schemas/" + filename)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}

	codec, err := avro.ParseSchema(string(bytes))
	if err != nil {
		t.Fatalf("parsing schema: %v", err)
	}
	return codec
}
