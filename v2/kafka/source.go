package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-avro/avro"
	"github.com/pilosa/go-pilosa"
	pdk "github.com/pilosa/pdk/v2"
	"github.com/pkg/errors"
)

type Main struct {
	PilosaHosts      []string `help:"Comma separated list of host:port pairs for Pilosa."`
	KafkaHosts       []string `help:"Comma separated list of host:port pairs for Kafka."`
	RegistryURL      string   `help:"Location of Confluent Schema Registry"`
	BatchSize        int      `help:"Number of records to read before indexing all of them at once. Generally, larger means better throughput and more memory usage. 1,048,576 might be a good number."`
	Group            string   `help:"Kafka group."`
	Index            string   `help:"Name of Pilosa index."`
	Topics           []string `help:"Kafka topics to read from."`
	LogPath          string   `help:"Log file to write to. Empty means stderr."e`
	PrimaryKeyFields []string `help:"Data field(s) which make up the primary key for a record. These will be concatenated and translated to a Pilosa ID. If empty, record key translation will not be used."`
	IDField          string   `help:"Field which contains the integer column ID. May not be used in conjunction with primary-key-fields. If both are empty, auto-generated IDs will be used."`
	MaxMsgs          int      `help:"Number of messages to consume from Kafka before stopping. Useful for testing when you don't want to run indefinitely."`
	// TODO implement the auto-generated IDs... hopefully using Pilosa to manage it.
}

func NewMain() *Main {
	return &Main{
		PilosaHosts: []string{"localhost:10101"},
		KafkaHosts:  []string{"localhost:9092"},
		RegistryURL: "localhost:8081",
		BatchSize:   1, // definitely increase this to achieve any amount of performance
		Group:       "defaultgroup",
		Index:       "defaultindex",
		Topics:      []string{"defaulttopic"},
	}
}

func (m *Main) Run() error {
	if err := m.validate(); err != nil {
		return errors.Wrap(err, "validating configuration")
	}

	client, err := pilosa.NewClient(m.PilosaHosts)
	if err != nil {
		return errors.Wrap(err, "getting pilosa client")
	}
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}
	keyTranslation := len(m.PrimaryKeyFields) > 0
	index := schema.Index(m.Index, pilosa.OptIndexKeys(keyTranslation))
	fmt.Println(index)

	source := NewSource()
	source.Hosts = m.KafkaHosts
	source.Topics = m.Topics
	source.Group = m.Group
	source.MaxMsgs = m.MaxMsgs

	// remember to flush old batch and make a new batch when schema changes

	return nil
}

func (m *Main) validate() error {
	if len(m.PrimaryKeyFields) != 0 && m.IDField != "" {
		return errors.New("cannot set both primary key fields and id-field")
	}
	return nil
}

// Source implements the pdk.Source interface using kafka as a data
// source. It is not threadsafe! Due to the way Kafka clients work, to
// achieve concurrency, create multiple Sources.
type Source struct {
	Hosts       []string
	Topics      []string
	Group       string
	MaxMsgs     int
	RegistryURL string

	numMsgs  int
	consumer *cluster.Consumer
	messages <-chan *sarama.ConsumerMessage

	// lastSchemaID and lastSchema keep track of the most recent
	// schema in use. We expect this not to change often, but when it
	// does, we need to notify the caller of Source.Record()
	lastSchemaID int32
	lastSchema   []pdk.Field

	// cache is a schema cache so we don't have to look up the same
	// schema from the registry each time.
	cache map[int32]avro.Schema
	// stash is a local offset stash which source maintains so it can
	// control when offsets are committed to Kafka.
	stash *cluster.OffsetStash

	decBytes []byte
	record   *Record
}

// NewSource gets a new Source
func NewSource() *Source {
	src := &Source{
		Hosts:  []string{"localhost:9092"},
		Topics: []string{"test"},
		Group:  "group0",

		lastSchemaID: -1,
		cache:        make(map[int32]avro.Schema),
		stash:        cluster.NewOffsetStash(),

		decBytes: make([]byte, 8),
	}
	src.record = &Record{src: src}

	return src
}

// Record returns the value of the next kafka message. The same Record
// object may be used by successive calls to Record, so it should not
// be retained.
func (s *Source) Record() (pdk.Record, error) {
	if s.MaxMsgs > 0 {
		s.numMsgs++
		if s.numMsgs > s.MaxMsgs {
			return nil, io.EOF
		}
	}
	msg, ok := <-s.messages
	s.stash.MarkOffset(msg, "")
	if ok {
		val, err := s.decodeAvroValueWithSchemaRegistry(msg.Value)
		if err != nil && err != pdk.ErrSchemaChange {
			return nil, errors.Wrap(err, "decoding with schema registry")
		}
		if err == pdk.ErrSchemaChange {
			s.record.data = make([]interface{}, len(s.lastSchema))
		}
		recErr := s.toPDKRecord(val.(map[string]interface{})) // val must be map[string]interface{} because we only accept schemas which are Record type at the top level.
		if recErr != nil {
			// reset lastSchema so if Record gets called again, and
			// the schema just changed, we'll notify of the change.
			s.lastSchema = nil
			s.lastSchemaID = -1
			return nil, errors.Wrap(recErr, "converting to PDK Record")
		}
		return s.record, err // err must be nil or ErrSchemaChange at this point
	}
	return nil, errors.New("messages channel closed")
}

func (s *Source) Schema() []pdk.Field {
	return s.lastSchema
}

func (s *Source) toPDKRecord(vals map[string]interface{}) error {
	r := s.record
	for i, field := range s.lastSchema {
		val := vals[field.Name()]
		switch field.(type) {
		case pdk.DecimalField:
			vb, ok := val.([]byte)
			if !ok {
				return errors.Errorf("decimal must be []byte, but got %v of %[1]T", val)
			}
			if len(vb) == 8 {
				r.data[i] = binary.BigEndian.Uint64(vb)
			} else if len(vb) < 8 {
				copy(s.decBytes[8-len(vb):], vb)
				r.data[i] = binary.BigEndian.Uint64(s.decBytes)
				for i := 8 - len(vb); i >= 0; i-- {
					s.decBytes[i] = 0
				}
			} else {
				return errors.Errorf("can't support decimals of greater than 8 bytes, got %d for %s", len(vb), field.Name())
			}
		default:
			r.data[i] = val
		}
	}
	return nil
}

type Record struct {
	src  *Source
	data []interface{}
}

func (r *Record) Commit() error {
	r.src.consumer.MarkOffsets(r.src.stash)

	// TODO this can return temporary errors according to the
	// documentation. Might be good to detect those and retry in here.
	return r.src.consumer.CommitOffsets()
}

func (r *Record) Data() []interface{} {
	return r.data
}

// Open initializes the kafka source.
func (s *Source) Open() error {
	// init (custom) config, enable errors and notifications
	sarama.Logger = log.New(ioutil.Discard, "", 0) // TODO get logs?
	config := cluster.NewConfig()
	config.Config.Version = sarama.V0_10_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true

	var err error
	s.consumer, err = cluster.NewConsumer(s.Hosts, s.Group, s.Topics, config)
	if err != nil {
		return errors.Wrap(err, "getting new consumer")
	}
	s.messages = s.consumer.Messages()

	// consume errors
	go func() {
		for err := range s.consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range s.consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()
	return nil
}

// Close closes the underlying kafka consumer.
func (s *Source) Close() error {
	err := s.consumer.Close()
	return errors.Wrap(err, "closing kafka consumer")
}

// TODO change name
func (s *Source) decodeAvroValueWithSchemaRegistry(val []byte) (interface{}, error) {
	if len(val) <= 6 || val[0] != 0 {
		return nil, errors.Errorf("unexpected magic byte or length in avro kafka value, should be 0x00, but got 0x%.8s", val)
	}
	id := int32(binary.BigEndian.Uint32(val[1:]))
	codec, err := s.getCodec(id)
	if err != nil {
		return nil, errors.Wrap(err, "getting avro codec")
	}
	ret, err := avroDecode(codec, val[5:])
	if err != nil {
		return nil, errors.Wrap(err, "decoding avro record")
	}
	if id != s.lastSchemaID {
		s.lastSchema, err = avroToPDKSchema(codec)
		if err != nil {
			return nil, errors.Wrap(err, "converting to Pilosa schema")
		}
		s.lastSchemaID = id
		return ret, pdk.ErrSchemaChange
	}

	return ret, nil
}

// avroToPDKSchema converts a full avro schema to the much more
// constrained []pdk.Field which maps pretty directly onto
// Pilosa. Many features of avro are unsupported and will cause this
// to return an error. The "codec" argument ot this function must be
// an avro.Record.
func avroToPDKSchema(codec avro.Schema) ([]pdk.Field, error) {
	switch codec.Type() {
	case avro.Record:
		recordSchema, ok := codec.(*avro.RecordSchema)
		if !ok {
			panic(fmt.Sprintf("Record isn't a *avro.RecordSchema, got %+v of %[1]T", codec))
		}
		pdkFields := make([]pdk.Field, 0, len(recordSchema.Fields))
		for _, field := range recordSchema.Fields {
			pdkField, err := avroToPDKField(field)
			if err != nil {
				return nil, errors.Wrap(err, "converting avro field to pdk")
			}
			pdkFields = append(pdkFields, pdkField)
		}
		return pdkFields, nil
	default:
		return nil, errors.Errorf("unsupported Avro Schema type: %d", codec.Type()) // TODO error msg with int type is pretty opaque
	}

}

func avroToPDKField(aField *avro.SchemaField) (pdk.Field, error) {
	switch aField.Type.Type() {
	case avro.Record:
		return nil, errors.Errorf("nested fields are not currently supported, so the field type cannot be record.")
	case avro.Enum, avro.String:
		pdkField := pdk.StringField{NameVal: aField.Name}
		if mutex, ok := aField.Prop("mutex"); ok {
			if mutexb, ok := mutex.(bool); ok {
				pdkField.Mutex = mutexb // TODO document this behavior
			}
		}
		return pdkField, nil
	case avro.Bytes, avro.Fixed:
		if lt, _ := stringProp(aField, "logicalType"); lt == "decimal" {
			precision, err := intProp(aField, "precision")
			if precision > 18 || precision < 1 {
				return nil, errors.Errorf("need precision for decimal in 1-18, but got:%d, err:%v", precision, err)
			}
			scale, err := intProp(aField, "scale")
			if scale > precision || err == wrongType {
				return nil, errors.Errorf("0<=scale<=precision, got:%d err:%v", scale, err)
			}
			return pdk.DecimalField{
				NameVal: aField.Name,
				Scale:   uint(scale),
			}, nil
		}
		// if not a decimal, then treat as string
		pdkField := pdk.StringField{NameVal: aField.Name}
		if mutex, ok := aField.Prop("mutex"); ok {
			if mutexb, ok := mutex.(bool); ok {
				pdkField.Mutex = mutexb // TODO document this behavior
			}
		}
		return pdkField, nil
	case avro.Union:
		return avroUnionToPDKField(aField)
	case avro.Array:
		itemSchema := aField.Type.(*avro.ArraySchema).Items
		switch typ := itemSchema.Type(); typ {
		case avro.String, avro.Bytes, avro.Fixed, avro.Enum:
			if lt, _ := stringProp(itemSchema, "logicalType"); lt == "decimal" {
				return nil, errors.New("arrays of decimal are not supported")
			}
			return pdk.StringArrayField{NameVal: aField.Name}, nil
		default:
			return nil, errors.Errorf("array items type of %d is unsupported", itemSchema.Type())
		}
	case avro.Int, avro.Long:
		if lt, _ := stringProp(aField, "logicalType"); lt == "PilosaID" {
			return pdk.IDField{
				NameVal: aField.Name,
			}, nil
		}
		return pdk.IntField{
			NameVal: aField.Name,
		}, nil
	case avro.Float, avro.Double:
		return pdk.IntField{
			NameVal: aField.Name,
		}, nil
	case avro.Boolean:
		return pdk.BoolField{
			NameVal: aField.Name,
		}, nil
	case avro.Null:
		return nil, errors.Errorf("null fields are not supported except inside Union")
	case avro.Map:
		return nil, errors.Errorf("nested fields are not currently supported, so the field type cannot be map.")
	case avro.Recursive:
		return nil, errors.Errorf("recursive schema fields are not currently supported.")
	default:
		return nil, errors.Errorf("unknown schema type: %+v", aField.Type)
	}

}

func stringProp(p propper, s string) (string, error) {
	ival, ok := p.Prop(s)
	if !ok {
		return "", notFound
	}
	sval, ok := ival.(string)
	if !ok {
		return "", wrongType
	}
	return sval, nil
}

func intProp(p propper, s string) (int, error) {
	ival, ok := p.Prop(s)
	if !ok {
		return 0, notFound
	}
	// json decodes numeric values into float64
	floatVal, ok := ival.(float64)
	if !ok {
		return 0, wrongType
	}
	return int(floatVal), nil
}

type propper interface {
	Prop(string) (interface{}, bool)
}

var notFound = errors.New("prop not found")
var wrongType = errors.New("val is wrong type")

// avroUnionToPDKField takes an avro SchemaField with a Union type,
// and reduces it to a SchemaField with the type of one of the Types
// contained in the Union. It can only do this if the Union only has
// one type, or if it has two types and one is null.
func avroUnionToPDKField(field *avro.SchemaField) (pdk.Field, error) {
	if field.Type.Type() != avro.Union {
		panic("it should be impossible to call avroUnionToPDKField with a non-union SchemaField")
	}
	uSchema := field.Type.(*avro.UnionSchema)
	nf := &avro.SchemaField{
		Name:    field.Name,
		Doc:     field.Doc,
		Default: field.Default,
	}
	if len(uSchema.Types) == 1 {
		nf.Type = uSchema.Types[0]
		return avroToPDKField(nf)
	}
	if len(uSchema.Types) == 2 {
		var useType avro.Schema
		if uSchema.Types[0].Type() == avro.Null {
			useType = uSchema.Types[1]
		} else if uSchema.Types[1].Type() == avro.Null {
			useType = uSchema.Types[0]
		} else {
			return nil, errors.New("unions are only supported when one type is Null")
		}
		nf.Type = useType
		nf.Properties = propertiesFromSchema(useType)
		return avroToPDKField(nf)
	}
	return nil, errors.New("unions are only supported when they are a single type plus optionally a Null")
}

// propertiesFromSchema (document and use!)
func propertiesFromSchema(sch avro.Schema) map[string]interface{} {
	switch schT := sch.(type) {
	case *avro.StringSchema, *avro.IntSchema, *avro.LongSchema, *avro.FloatSchema, *avro.DoubleSchema, *avro.BooleanSchema, *avro.NullSchema, *avro.UnionSchema:
		return nil
	case *avro.BytesSchema:
		return schT.Properties
	case *avro.RecordSchema:
		return schT.Properties
	case *avro.RecursiveSchema:
		if schT.Actual != nil {
			return schT.Actual.Properties
		}
		return nil
	case *avro.EnumSchema:
		return schT.Properties
	case *avro.ArraySchema:
		return schT.Properties
	case *avro.MapSchema:
		return schT.Properties
	case *avro.FixedSchema:
		return schT.Properties
	default:
		// TODO handle logging properly (e.g. respect log path, use an interface logger, etc.)
		log.Printf("unhandled avro.Schema concrete type %T in propertiesFromSchema, value: %+v", schT, schT)
		return nil
	}
}

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	ID      int    `json:"id"`      // Registry's unique id
}

func (s *Source) getCodec(id int32) (rschema avro.Schema, rerr error) {
	if codec, ok := s.cache[id]; ok {
		return codec, nil
	}

	r, err := http.Get(fmt.Sprintf("http://%s/schemas/ids/%d", s.RegistryURL, id))
	if err != nil {
		return nil, errors.Wrap(err, "getting schema from registry")
	}
	defer func() {
		// TODO this might obscure a more important error?
		rerr = r.Body.Close()
	}()
	if r.StatusCode >= 300 {
		bod, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get schema, code: %d, no body", r.StatusCode)
		}
		return nil, errors.Errorf("Failed to get schema, code: %d, resp: %s", r.StatusCode, bod)
	}
	dec := json.NewDecoder(r.Body)
	schema := &Schema{}
	err = dec.Decode(schema)
	if err != nil {
		return nil, errors.Wrap(err, "decoding schema from registry")
	}
	codec, err := avro.ParseSchema(schema.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "parsing schema")
	}
	s.cache[id] = codec
	return codec, rerr
}

func avroDecode(codec avro.Schema, data []byte) (map[string]interface{}, error) {
	reader := avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(codec)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(data)

	decodedRecord := avro.NewGenericRecord(codec)
	// Read data into given GenericRecord with a given Decoder. The first parameter to Read should be something to read into
	err := reader.Read(decodedRecord, decoder)
	if err != nil {
		return nil, errors.Wrap(err, "reading generic datum")
	}

	return decodedRecord.Map(), nil
}

func toUint64(val interface{}) (uint64, error) {
	switch vt := val.(type) {
	case uint:
		return uint64(vt), nil
	case uint8:
		return uint64(vt), nil
	case uint16:
		return uint64(vt), nil
	case uint32:
		return uint64(vt), nil
	case uint64:
		return vt, nil
	case int:
		return uint64(vt), nil
	case int8:
		return uint64(vt), nil
	case int16:
		return uint64(vt), nil
	case int32:
		return uint64(vt), nil
	case int64:
		return uint64(vt), nil
	default:
		return 0, errors.Errorf("couldn't convert %v of %[1]T to uint64", vt)
	}
}
