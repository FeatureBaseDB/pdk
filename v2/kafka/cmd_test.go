package kafka

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk/v2"
	"github.com/pilosa/pilosa/logger"
)

func TestCmdMainOne(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// TODO automate the setup for this test (creating certs with certstrap, etc)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("getting home dir: %v", err)
	}

	tests := []struct {
		name             string
		PrimaryKeyFields []string
		IDField          string
		PilosaHosts      []string
		RegistryURL      string
		TLS              *pdk.TLSConfig
		expRhinoKeys     []string
		expRhinoCols     []uint64
	}{
		{
			name:             "3 primary keys str/str/int",
			PrimaryKeyFields: []string{"abc", "db", "user_id"},
			expRhinoKeys:     []string{string([]byte{50, 49, 0, 0, 0, 159})}, // "2" + "1" + uint32(159)

		},
		{
			name:             "3 primary keys str/str/int",
			PrimaryKeyFields: []string{"abc", "db", "user_id"},
			PilosaHosts:      []string{"https://localhost:10111"},
			TLS: &pdk.TLSConfig{
				CertificatePath:          home + "/pilosa-sec/out/theclient.crt",
				CertificateKeyPath:       home + "/pilosa-sec/out/theclient.key",
				CACertPath:               home + "/pilosa-sec/out/ca.crt",
				EnableClientVerification: true,
			},
			expRhinoKeys: []string{string([]byte{50, 49, 0, 0, 0, 159})}, // "2" + "1" + uint32(159)

		},
		{
			name:         "IDField int",
			IDField:      "user_id",
			expRhinoCols: []uint64{159},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields := []string{"abc", "db", "user_id", "all_users", "has_deleted_date", "central_group", "custom_audiences", "desktop_boolean", "desktop_frequency", "desktop_recency", "product_boolean_historical_forestry_cravings_or_bugles", "ddd_category_total_current_rhinocerous_checking", "ddd_category_total_current_rhinocerous_thedog_cheetah", "survey1234", "days_since_last_logon", "elephant_added_for_account"}

			records := [][]interface{}{
				{"2", "1", 159, map[string]interface{}{"boolean": true}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
			}

			a := rand.Int()
			topic := strconv.Itoa(a)

			// make a bunch of data and insert it

			// create Main and run with MaxMsgs
			m := NewMain()
			m.Index = fmt.Sprintf("cmd_test_index239ij%s", topic)
			m.PrimaryKeyFields = test.PrimaryKeyFields
			m.IDField = test.IDField
			m.PackBools = "bools"
			m.BatchSize = 1
			m.Topics = []string{topic}
			m.MaxMsgs = len(records)
			if test.PilosaHosts != nil {
				m.PilosaHosts = test.PilosaHosts
			}
			if test.TLS != nil {
				m.TLS = *test.TLS
			}
			if test.RegistryURL != "" {
				m.RegistryURL = test.RegistryURL
			}

			// load big schema
			licodec := liDecodeTestSchema(t, "bigschema.json")
			tlsConf, err := pdk.GetTLSConfig(test.TLS, logger.NopLogger)
			if err != nil {
				t.Fatalf("getting tls config: %v", err)
			}
			schemaID := postSchema(t, "bigschema.json", "bigschema2", m.RegistryURL, tlsConf)

			// put records in kafka
			conf := sarama.NewConfig()
			conf.Version = sarama.V0_10_0_0 // TODO - do we need this? should we move it up?
			conf.Producer.Return.Successes = true
			producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, conf)
			if err != nil {
				t.Fatalf("getting new producer: %v", err)
			}

			for _, vals := range records {
				rec := makeRecord(t, fields, vals)
				putRecordKafka(t, producer, schemaID, licodec, "akey", topic, rec)
			}

			err = m.Run()
			if err != nil {
				t.Fatalf("running main: %v", err)
			}

			client := m.PilosaClient()
			schema, err := client.Schema()
			index := schema.Index(m.Index)
			defer func() {
				err := client.DeleteIndex(index)
				if err != nil {
					t.Logf("deleting index: %v", err)
				}
			}()

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

			bools := index.Field("bools")
			qr, err = client.Query(bools.TopN(10))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			ci := sortableCRI(qr.Result().CountItems())
			exp := sortableCRI{{Count: 1, Key: "all_users"}}
			sort.Sort(ci)
			sort.Sort(exp)
			if !reflect.DeepEqual(ci, exp) {
				t.Errorf("unexpected result exp/got\n%+v\n%+v", exp, ci)
			}

			bools = index.Field("bools-exists")
			qr, err = client.Query(bools.TopN(10))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			ci = sortableCRI(qr.Result().CountItems())
			exp = sortableCRI{{Count: 1, Key: "all_users"}, {Count: 1, Key: "has_deleted_date"}}
			sort.Sort(ci)
			sort.Sort(exp)
			if !reflect.DeepEqual(ci, exp) {
				t.Errorf("unexpected result exp/got\n%+v\n%+v", exp, ci)
			}

			rhino := index.Field("ddd_category_total_current_rhinocerous_checking")
			qr, err = client.Query(rhino.Equals(540))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			if test.expRhinoKeys != nil {
				if keys := qr.Result().Row().Keys; !reflect.DeepEqual(keys, test.expRhinoKeys) {
					t.Errorf("wrong cols: %v, exp: %v", keys, test.expRhinoKeys)
				}
			}
			if test.expRhinoCols != nil {
				if cols := qr.Result().Row().Columns; !reflect.DeepEqual(cols, test.expRhinoCols) {
					t.Errorf("wrong cols: %v, exp: %v", cols, test.expRhinoCols)
				}
			}
		})
	}
}

type sortableCRI []pilosa.CountResultItem

func (s sortableCRI) Len() int { return len(s) }
func (s sortableCRI) Less(i, j int) bool {
	if s[i].Count != s[j].Count {
		return s[i].Count > s[j].Count
	}
	if s[i].ID != s[j].ID {
		return s[i].ID < s[j].ID
	}
	if s[i].Key != s[j].Key {
		return s[i].Key < s[j].Key
	}
	return true
}
func (s sortableCRI) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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
