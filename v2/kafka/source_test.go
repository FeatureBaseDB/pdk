package kafka

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-avro/avro"
	liavro "github.com/linkedin/goavro/v2"
	pdk "github.com/pilosa/pdk/v2"
	"github.com/pilosa/pdk/v2/kafka/csrc"
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
			schemaFile: "floatscale.json",
			exp:        expectedSchemas["floatscale.json"],
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
	if len(files) != len(tests)+1 { // +1 because we aren't testing bigschema.json here
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

var tests = []struct {
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
		exp:        [][]interface{}{{[]byte{0}}, {[]byte{0x3, 0xE8}}, {[]byte{100}}, {[]byte{0, 250}}, {[]byte{0x1C, 0xBE, 0x99, 0x1A, 0x08}}},
	},
	{
		schemaFile: "othertypes.json",
		data:       []map[string]interface{}{{"first": "a", "second": []string{"b", "c"}, "third": -8, "fourth": 99, "fifth": 99.9, "sixth": 101.1, "seventh": true}},
		exp:        [][]interface{}{{"a", []interface{}{"b", "c"}, int32(-8), int64(99), float32(99.9), float64(101.1), true}},
	},
	{
		schemaFile: "unions.json",
		data: []map[string]interface{}{
			{"first": map[string]interface{}{"string": "a"}, "second": map[string]interface{}{"boolean": true}, "third": map[string]interface{}{"long": 101}, "fourth": map[string]interface{}{"bytes.decimal": big.NewRat(5, 2)}, "fifth": map[string]interface{}{"double": float64(9.4921)}},
			{"first": nil, "second": nil, "third": map[string]interface{}{"null": nil}, "fourth": nil, "fifth": nil},
		},
		exp: [][]interface{}{
			{"a", true, int64(101), []byte{9, 196}, float64(9.4921)},
			{nil, nil, nil, nil, nil}},
	},
	{
		schemaFile: "floatscale.json",
		data:       []map[string]interface{}{{"first": 23.12345}},
		exp:        [][]interface{}{{float32(23.12345)}},
	},
}

func TestKafkaSourceLocal(t *testing.T) {
	// this is not an integration test, so we'll take steps to avoid
	// actually connecting to Kafka or Schema Registry.

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
						t.Errorf("unexpected schema exp/got:\n%+v\n%+v", expectedSchemas[test.schemaFile], gotSchema)
					}
				} else if err != nil {
					t.Fatalf("unexpected error getting record: %v", err)
				}
				if pdkRec == nil {
					t.Fatalf("should have a record")
				}
				data := pdkRec.Data()
				if !reflect.DeepEqual(data, test.exp[j]) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v", test.exp[j], data)
					if len(data) != len(test.exp[j]) {
						t.Fatalf("mismatched lengths exp/got %d/%d", len(test.exp[j]), len(data))
					}
					for k := range test.exp[j] {
						if !reflect.DeepEqual(test.exp[j][k], data[k]) {
							t.Errorf("Mismatch at %d, exp/got\n%v of %[2]T\n%v of %[3]T", k, test.exp[j][k], data[k])
						}
					}
				}
			}
		})
	}

}

// TestKafkaSource uses a real Kafka and Schema Registry. I downloaded
// the tar archive of the Confluent Platform (self managed software)
// from confluent.io/download (I got version 5.3.1). I ran `tar xzf`
// on the file, changed into the directory, ran `curl -L
// https://cnfl.io/cli | sh -s -- -b /Users/jaffee/bin` (that
// directory is on my PATH), then ran `confluent local start
// schema-registry`.
//
// I find that this test runs much faster after a `confluent local
// destroy` followed by `confluent local start schema-registry`. The
// difference is stark—10s of seconds—and I don't know why this should
// be, but I think it has something to do with kafka rebalancing
// itself when a new client joins.
func TestKafkaSourceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	src := NewSource()
	src.Topics = []string{"testKafkaSourceIntegration"}
	src.Group = "group0"
	err := src.Open()
	if err != nil {
		t.Fatalf("opening source: %v", err)
	}

	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_0_0 // TODO - do we need this? should we move it up?
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, conf)
	if err != nil {
		t.Fatalf("getting new producer: %v", err)
	}
	defer producer.Close()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	key := fmt.Sprintf("%d", rnd.Int())
	for i, test := range tests {
		schemaID := postSchema(t, test.schemaFile, fmt.Sprintf("schema%d", i), "localhost:8081", nil)
		schema := liDecodeTestSchema(t, test.schemaFile)
		t.Run(test.schemaFile, func(t *testing.T) {

			for j, record := range test.data {
				putRecordKafka(t, producer, schemaID, schema, key, src.Topics[0], record)
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
				data := pdkRec.Data()
				if !reflect.DeepEqual(data, test.exp[j]) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v", test.exp[j], data)
					if len(data) != len(test.exp[j]) {
						t.Fatalf("mismatched lengths exp/got %d/%d", len(test.exp[j]), len(data))
					}
					for k := range test.exp[j] {
						if !reflect.DeepEqual(test.exp[j][k], data[k]) {
							t.Errorf("Mismatch at %d, exp/got\n%v of %[2]T\n%v of %[3]T", k, test.exp[j][k], data[k])
						}
					}

				}
			}
		})
	}

}

func postSchema(t *testing.T, schemaFile, subj, regURL string, tlsConfig *tls.Config) (schemaID int) {
	schemaClient := csrc.NewClient(regURL, tlsConfig)
	schemaStr := readTestSchema(t, schemaFile)
	resp, err := schemaClient.PostSubjects(subj, schemaStr)
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}
	return resp.ID
}

func putRecordKafka(t *testing.T, producer sarama.SyncProducer, schemaID int, schema *liavro.Codec, key, topic string, record map[string]interface{}) {
	t.Helper()
	buf := make([]byte, 5, 1000)
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
	buf, err := schema.BinaryFromNative(buf, record)
	if err != nil {
		t.Errorf("encoding:\n%+v\nerr: %v", record, err)
	}

	// post buf to kafka
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(buf)})
	if err != nil {
		t.Fatalf("sending message to kafka: %v", err)
	}
}

var expectedSchemas = map[string][]pdk.Field{
	"simple.json":      []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.StringField{NameVal: "last"}},
	"stringtypes.json": []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.StringField{NameVal: "last"}, pdk.StringField{NameVal: "middle"}},
	"decimal.json":     []pdk.Field{pdk.DecimalField{NameVal: "somenum", Scale: 2}},
	"unions.json":      []pdk.Field{pdk.StringField{NameVal: "first"}, pdk.BoolField{NameVal: "second"}, pdk.IntField{NameVal: "third"}, pdk.DecimalField{NameVal: "fourth", Scale: 3}, pdk.DecimalField{NameVal: "fifth", Scale: 2}},
	"othertypes.json":  []pdk.Field{pdk.StringField{NameVal: "first", Mutex: true}, pdk.StringArrayField{NameVal: "second"}, pdk.IntField{NameVal: "third"}, pdk.IntField{NameVal: "fourth"}, pdk.DecimalField{NameVal: "fifth"}, pdk.DecimalField{NameVal: "sixth"}, pdk.BoolField{NameVal: "seventh"}},
	"floatscale.json":  []pdk.Field{pdk.DecimalField{NameVal: "first", Scale: 4}},
}
