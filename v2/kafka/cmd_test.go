package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestCmdMain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// load big schema
	licodec := liDecodeTestSchema(t, "bigschema.json")
	schemaID := postSchema(t, "bigschema.json", "bigschema2")

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
		t.Fatalf("getting new producer: %v", err)
	}
	topic := "testcmdmain"
	for _, vals := range records {
		rec := makeRecord(t, fields, vals)
		putRecordKafka(t, producer, schemaID, licodec, "akey", topic, rec)
	}

	// create Main and run with MaxMsgs
	m := NewMain()
	m.Index = "cmd_test_index23lkjdkfj"
	m.PrimaryKeyFields = []string{"abc", "db", "user_id"}
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.MaxMsgs = len(records)

	err = m.Run()
	if err != nil {
		t.Fatalf("running main: %v", err)
	}

	client := m.PilosaClient()
	schema, err := client.Schema()
	index := schema.Index(m.Index)

	// check data in Pilosa
	if !index.HasField("abc") {
		t.Fatalf("don't have abc")
	}
	abc := index.Field("abc")
	qr, err := client.Query(index.Count(abc.Row("2")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if qr.Result().Count() != 1 {
		t.Fatalf("wrong count for abc, %d is not 1", qr.Result().Count())
	}
}

func makeRecord(t *testing.T, fields []string, vals []interface{}) map[string]interface{} {
	if len(fields) != len(vals) {
		t.Fatalf("have %d fields and %d vals", len(fields), len(vals))
	}
	ret := make(map[string]interface{})
	for i, field := range fields {
		ret[field] = vals[i]
	}
	return ret
}
