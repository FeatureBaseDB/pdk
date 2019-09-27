package kafka

import (
	"encoding/binary"
	"io/ioutil"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/go-avro/avro"
	liavro "github.com/linkedin/goavro/v2"
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
			exp:        expectedSchemas["simple.json"],
		},
		{
			schemaFile: "stringtypes.json",
			exp:        expectedSchemas["stringtypes.json"],
		},
		{
			schemaFile: "decimal.json",
			exp:        expectedSchemas["decimal.json"],
		},
		{
			schemaFile: "othertypes.json",
			exp:        expectedSchemas["othertypes.json"],
		},
		{
			schemaFile: "unions.json",
			exp:        expectedSchemas["unions.json"],
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
		t.Errorf("have different number of schemas and tests: %d and %d\n%+v", len(files), len(tests), files)
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
	codec, err := avro.ParseSchema(readTestSchema(t, filename))
	if err != nil {
		t.Fatalf("parsing schema: %v", err)
	}
	return codec
}

func readTestSchema(t *testing.T, filename string) string {
	bytes, err := ioutil.ReadFile("./testdata/schemas/" + filename)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	return string(bytes)
}

func liDecodeTestSchema(t *testing.T, filename string) *liavro.Codec {
	codec, err := liavro.NewCodec(readTestSchema(t, filename))
	if err != nil {
		t.Fatalf("li parsing schema: %v", err)
	}
	return codec
}

func TestKafkaSource(t *testing.T) {
	// this is not an integration test, so we'll take steps to avoid
	// actually connecting to Kafka or Schema Registry.

	tests := []struct {
		data       []map[string]interface{}
		schemaFile string
		exp        [][]interface{}
	}{
		{
			schemaFile: "simple.json",
			data:       []map[string]interface{}{{"first": "hello", "last": "goodbye"}, {"first": "one", "last": "two"}},
			exp:        [][]interface{}{{"hello", "goodbye"}, {"one", "two"}},
		},
		{
			schemaFile: "stringtypes.json",
			data:       []map[string]interface{}{{"first": "blah", "last": "goodbye", "middle": "123456789"}},
			exp:        [][]interface{}{{"blah", []byte("goodbye"), []byte("123456789")}},
		},
		{
			schemaFile: "decimal.json",
			data:       []map[string]interface{}{{"somenum": &big.Rat{}}, {"somenum": big.NewRat(10, 1)}, {"somenum": big.NewRat(1, 1)}, {"somenum": big.NewRat(5, 2)}, {"somenum": big.NewRat(1234567890, 1)}},
			exp:        [][]interface{}{[]interface{}{uint64(0)}, {uint64(1000)}, {uint64(100)}, {uint64(250)}, {uint64(123456789000)}},
		},
		// {
		// 	schemaFile: "othertypes.json",
		// 	data:       []map[string]interface{}{},
		// },
		// {
		// 	schemaFile: "unions.json",
		// 	data:       []map[string]interface{}{},
		// },
	}

	src := NewSource()
	// note: we will not call Open on the source which would connect
	// to Kafka. Instead, we'll set the src.messages manually so we
	// can inject messages.
	messages := make(chan *sarama.ConsumerMessage, 10)
	src.messages = messages

	for i, test := range tests {
		i := i
		schema := liDecodeTestSchema(t, test.schemaFile)

		// prefill the schema cache so the registry isn't contacted.
		src.cache[int32(i)] = decodeTestSchema(t, test.schemaFile)
		t.Run(test.schemaFile, func(t *testing.T) {

			for j, record := range test.data {
				buf := make([]byte, 5, 1000)
				buf[0] = 0
				binary.BigEndian.PutUint32(buf[1:], uint32(i))
				buf, err := schema.BinaryFromNative(buf, record)
				if err != nil {
					t.Errorf("encoding:\n%+v\nerr: %v", record, err)
				}

				messages <- &sarama.ConsumerMessage{Value: buf}
				pdkRec, err := src.Record()
				if j == 0 {
					if err != pdk.ErrSchemaChange {
						t.Errorf("expected schema changed signal, got: %v", err)
					}
					gotSchema := src.Schema()
					if !reflect.DeepEqual(gotSchema, expectedSchemas[test.schemaFile]) {
						t.Errorf("unexpected schema got/exp:\n%+v\n%+v", gotSchema, expectedSchemas[test.schemaFile])
					}
				} else if err != nil {
					t.Fatalf("unexpected error getting record: %v", err)
				}
				if pdkRec == nil {
					t.Fatalf("should have a record")
				}
				if !reflect.DeepEqual(pdkRec.Data(), test.exp[j]) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v\n%[1]T %[2]T", test.exp[j][0], pdkRec.Data()[0])
				}
			}
		})
	}

}

var expectedSchemas = map[string][]pdk.Field{
	"simple.json":      []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.StringField{NameVal: "last"}},
	"stringtypes.json": []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.StringField{NameVal: "last"}, pdk.StringField{NameVal: "middle"}},
	"decimal.json":     []pdk.Field{pdk.DecimalField{NameVal: "somenum", Scale: 2}},
	"unions.json":      []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.BoolField{NameVal: "second"}, pdk.IntField{NameVal: "third"}, pdk.DecimalField{NameVal: "fourth", Scale: 3}},
	"othertypes.json":  []pdk.Field{pdk.StringField{NameVal: "first", Mutex: true}, pdk.StringArrayField{NameVal: "second"}, pdk.IntField{NameVal: "third"}, pdk.IntField{NameVal: "fourth"}, pdk.IntField{NameVal: "fifth"}, pdk.IntField{NameVal: "sixth"}, pdk.BoolField{NameVal: "seventh"}},
}
