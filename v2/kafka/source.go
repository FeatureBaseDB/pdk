package kafka

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-avro/avro"
	pdk "github.com/pilosa/pdk/v2"
	"github.com/pilosa/pilosa/logger"
	"github.com/pkg/errors"
)

// Source implements the pdk.Source interface using kafka as a data
// source. It is not threadsafe! Due to the way Kafka clients work, to
// achieve concurrency, create multiple Sources.
type Source struct {
	Hosts       []string
	Topics      []string
	Group       string
	MaxMsgs     int
	RegistryURL string
	TLS         pdk.TLSConfig
	Log         logger.Logger

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
	stash      *cluster.OffsetStash
	httpClient *http.Client

	decBytes []byte
	record   *Record
}

// NewSource gets a new Source
func NewSource() *Source {
	src := &Source{
		Hosts:       []string{"localhost:9092"},
		Topics:      []string{"test"},
		Group:       "group0",
		RegistryURL: "http://localhost:8081",
		Log:         logger.NopLogger,

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
		r.data[i] = vals[field.Name()]
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
	config.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	config.Consumer.Group.Session.Timeout = time.Second

	if !strings.HasPrefix(s.RegistryURL, "http") {
		s.RegistryURL = "http://" + s.RegistryURL
	}
	s.httpClient = http.DefaultClient

	if s.TLS.CertificatePath != "" {
		tlsConfig, err := pdk.GetTLSConfig(&s.TLS, s.Log)
		if err != nil {
			return errors.Wrap(err, "getting TLS config")
		}
		config.Config.Net.TLS.Config = tlsConfig
		config.Config.Net.TLS.Enable = true

		if strings.HasPrefix(s.RegistryURL, "https://") {
			s.httpClient = getHTTPClient(tlsConfig)
		}
	}

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
		// TODO should probably require a logicalType if we're going
		// to treat a float as a decimal.
		field := pdk.DecimalField{
			NameVal: aField.Name,
		}
		scale, err := intProp(aField, "scale")
		if err == wrongType {
			return nil, errors.Wrap(err, "getting scale")
		} else if err == nil {
			field.Scale = uint(scale)
		}
		return field, nil
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
	case *avro.StringSchema, *avro.IntSchema, *avro.LongSchema, *avro.BooleanSchema, *avro.NullSchema, *avro.UnionSchema:
		return nil
	case *avro.BytesSchema:
		return schT.Properties
	case *avro.DoubleSchema:
		return schT.Properties
	case *avro.FloatSchema:
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

	r, err := s.httpClient.Get(fmt.Sprintf("%s/schemas/ids/%d", s.RegistryURL, id))
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

func getHTTPClient(t *tls.Config) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 20 * time.Second,
			DualStack: true,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}
	return &http.Client{Transport: transport}
}
