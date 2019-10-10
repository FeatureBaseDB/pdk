package kafkagen

import (
	"encoding/binary"
	"io/ioutil"

	"github.com/Shopify/sarama"
	liavro "github.com/linkedin/goavro/v2"
	"github.com/pilosa/pdk/v2/kafka/csrc"
	"github.com/pkg/errors"
)

type Main struct {
	SchemaFile  string
	Subject     string
	RegistryURL string
	KafkaHosts  []string
	Topic       string
}

func NewMain() *Main {
	return &Main{
		SchemaFile:  "bigschema.json",
		Subject:     "bigschema",
		RegistryURL: "localhost:8081",
		KafkaHosts:  []string{"localhost:9092"},
		Topic:       "defaulttopic",
	}
}

func (m *Main) Run() error {

	licodec, err := decodeSchema(m.SchemaFile)
	if err != nil {
		return errors.Wrap(err, "decoding schema")
	}
	schemaID, err := m.postSchema(m.SchemaFile, m.Subject)
	if err != nil {
		return errors.Wrap(err, "psting schema")
	}

	fields := []string{"abc", "db", "user_id", "all_users", "has_deleted_date", "central_group", "custom_audiences", "desktop_boolean", "desktop_frequency", "desktop_recency", "product_boolean_historical_forestry_cravings_or_bugles", "ddd_category_total_current_rhinocerous_checking", "ddd_category_total_current_rhinocerous_thedog_cheetah", "survey1234", "days_since_last_logon", "elephant_added_for_account"}

	// make a bunch of data and insert it
	records := [][]interface{}{
		{"2", "1", 159, map[string]interface{}{"boolean": true}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
	}

	// put records in kafka
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_0_0 // TODO - do we need this? should we move it up?
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, conf)
	if err != nil {
		return err
	}
	for _, vals := range records {
		rec, err := makeRecord(fields, vals)
		if err != nil {
			return errors.Wrap(err, "making record")
		}
		err = putRecordKafka(producer, schemaID, licodec, "akey", m.Topic, rec)
		if err != nil {
			return errors.Wrap(err, "putting record")
		}
	}
	return nil
}

func putRecordKafka(producer sarama.SyncProducer, schemaID int, schema *liavro.Codec, key, topic string, record map[string]interface{}) error {
	buf := make([]byte, 5, 1000)
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
	buf, err := schema.BinaryFromNative(buf, record)
	if err != nil {
		return err
	}

	// post buf to kafka
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(buf)})
	if err != nil {
		return err
	}
	return nil
}

func readSchema(filename string) (string, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func decodeSchema(filename string) (*liavro.Codec, error) {
	s, err := readSchema(filename)
	if err != nil {
		return nil, errors.Wrap(err, "reading schema")
	}
	codec, err := liavro.NewCodec(s)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

func (m *Main) postSchema(schemaFile, subj string) (schemaID int, err error) {
	schemaClient := csrc.NewClient("http://" + m.RegistryURL)
	schemaStr, err := readSchema(schemaFile)
	if err != nil {
		return 0, errors.Wrap(err, "reading schema file")
	}
	resp, err := schemaClient.PostSubjects(subj, schemaStr)
	if err != nil {
		return 0, errors.Wrap(err, "posting schema")
	}
	return resp.ID, nil
}

func makeRecord(fields []string, vals []interface{}) (map[string]interface{}, error) {
	if len(fields) != len(vals) {
		return nil, errors.Errorf("have %d fields and %d vals", len(fields), len(vals))
	}
	ret := make(map[string]interface{})
	for i, field := range fields {
		ret[field] = vals[i]
	}
	return ret, nil
}
