package kafka

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pilosa/go-pilosa/gpexp"
	pdk "github.com/pilosa/pdk/v2"
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

	// check data in Pilosa
	if !m.index.HasField("abc") {
		t.Fatalf("don't have abc")
	}
	abc := m.index.Field("abc")
	qr, err := m.client.Query(m.index.Count(abc.Row("2")))
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

func TestGetPrimaryKeyRecordizer(t *testing.T) {
	tests := []struct {
		name     string
		schema   []pdk.Field
		pkFields []string
		expErr   string
		expSkip  map[int]struct{}
		rawRec   []interface{}
		expID    interface{}
	}{
		{
			name:   "no schema",
			expErr: "can't call getPrimaryKeyRecordizer with empty schema",
		},
		{
			name:   "no pkfields",
			schema: []pdk.Field{pdk.StringField{}},
			expErr: "can't call getPrimaryKeyRecordizer with empty pkFields",
		},
		{
			name:     "primary is StringArray",
			schema:   []pdk.Field{pdk.StringArrayField{NameVal: "blah"}},
			pkFields: []string{"blah"},
			expErr:   "field blah cannot be a primary key field because it is a StringArray field.",
		},
		{
			name:     "primary is StringArray complex",
			schema:   []pdk.Field{pdk.StringField{NameVal: "zaa"}, pdk.IntField{NameVal: "hey"}, pdk.StringArrayField{NameVal: "blah"}},
			pkFields: []string{"blah", "zaa"},
			expErr:   "field blah cannot be a primary key field because it is a StringArray field.",
		},
		{
			name:     "unknown pkfield",
			schema:   []pdk.Field{pdk.StringField{NameVal: "zaa"}},
			pkFields: []string{"zaa", "zz"},
			expErr:   "no field with primary key field name zz found",
		},
		{
			name:     "unknown pkfield complex",
			schema:   []pdk.Field{pdk.StringField{NameVal: "zaa"}, pdk.IntField{NameVal: "hey"}, pdk.StringField{NameVal: "blah"}},
			pkFields: []string{"blah", "zz", "zaa"},
			expErr:   "no field with primary key field name zz found",
		},
		{
			name:     "skip primary",
			schema:   []pdk.Field{pdk.StringField{NameVal: "a"}, pdk.IntField{NameVal: "b"}},
			pkFields: []string{"a"},
			expSkip:  map[int]struct{}{0: struct{}{}},
			rawRec:   []interface{}{"a", 9},
			expID:    []byte("a"),
		},
		{
			name:     "primaries as ints",
			schema:   []pdk.Field{pdk.StringField{NameVal: "a"}, pdk.IntField{NameVal: "b"}, pdk.IntField{NameVal: "c"}, pdk.IntField{NameVal: "d"}},
			pkFields: []string{"c", "d", "b"},
			rawRec:   []interface{}{"a", uint32(1), uint32(2), uint32(4)},
			expID:    []byte{0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rdz, skips, err := getPrimaryKeyRecordizer(test.schema, test.pkFields)
			if test.expErr != "" {
				if err == nil {
					t.Fatalf("nil err, expected %s", test.expErr)
				}
				if !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("unmatched errs exp/got\n%s\n%v", test.expErr, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(skips, test.expSkip) {
				t.Errorf("unmatched skips exp/got\n%+v\n%+v", test.expSkip, skips)
			}

			row := &gpexp.Row{}
			err = rdz(test.rawRec, row)
			if err != nil {
				t.Fatalf("unexpected error from recordizer: %v", err)
			}
			if !reflect.DeepEqual(test.expID, row.ID) {
				t.Fatalf("mismatched row IDs exp: %+v, got: %+v", test.expID, row.ID)
			}

		})
	}
}

func TestBatchFromSchema(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type testcase struct {
		name      string
		schema    []pdk.Field
		IDField   string
		pkFields  []string
		packBools string
		rawRec    []interface{}
		rowID     interface{}
		rowVals   []interface{}
		err       string
		batchErr  string
	}
	runTest := func(t *testing.T, test testcase, removeIndex bool) {
		m := NewMain()
		m.Index = "cmd_test_index23lkjdkfj"
		m.PrimaryKeyFields = test.pkFields
		m.IDField = test.IDField
		m.PackBools = test.packBools
		m.BatchSize = 2

		err := m.setup()
		if err != nil {
			t.Fatalf("%v", err)
		}
		if removeIndex {
			defer func() {
				err := m.client.DeleteIndex(m.index)
				if err != nil {
					t.Logf("deleting test index: %v", err)
				}
			}()
		}

		rdzs, batch, row, err := m.batchFromSchema(test.schema)
		if testErr(t, test.err, err) {
			return
		}

		for _, rdz := range rdzs {
			err = rdz(test.rawRec, row)
			if err != nil {
				t.Fatalf("recordizing: %v", err)
			}
		}

		if !reflect.DeepEqual(row.ID, test.rowID) {
			t.Fatalf("row IDs exp: %+v got %+v", test.rowID, row.ID)
		}
		if !reflect.DeepEqual(row.Values, test.rowVals) {
			t.Errorf("row values exp/got:\n%+v %[1]T\n%+v %[2]T", test.rowVals, row.Values)
			if len(row.Values) == len(test.rowVals) {
				for i, v := range row.Values {
					if !reflect.DeepEqual(v, test.rowVals[i]) {
						t.Errorf("%v %[1]T != %v %[2]T", test.rowVals[i], v)
					}
				}
			}
			t.Fail()
		}

		err = batch.Add(*row)
		if testErr(t, test.batchErr, err) {
			return
		}
	}

	tests := []testcase{
		{
			name: "empty",
			err:  "autogen IDs is currently unimplemented",
		},
		{
			name:    "no id field",
			schema:  []pdk.Field{pdk.StringField{}},
			IDField: "nope",
			err:     "ID field nope not found",
		},
		{
			name:     "pk error",
			pkFields: []string{"zoop"},
			err:      "getting primary key recordizer",
		},
		{
			name:      "pack bools",
			schema:    []pdk.Field{pdk.BoolField{NameVal: "a"}, pdk.IDField{NameVal: "b"}, pdk.BoolField{NameVal: "c"}},
			IDField:   "b",
			packBools: "bff",
			rawRec:    []interface{}{true, uint64(7), false},
			rowID:     uint64(7),
			rowVals:   []interface{}{"a", "a", nil, "c"},
		},
		{
			name:    "don't pack bools",
			schema:  []pdk.Field{pdk.BoolField{NameVal: "a"}, pdk.IDField{NameVal: "b"}, pdk.BoolField{NameVal: "c"}},
			IDField: "b",
			rawRec:  []interface{}{true, uint64(7), false},
			rowID:   uint64(7),
			rowVals: []interface{}{true, false},
			err:     "field type bool is not currently supported through Batch",
		},
		{
			name:    "mutex field",
			schema:  []pdk.Field{pdk.StringField{NameVal: "a", Mutex: true}, pdk.IDField{NameVal: "b"}},
			IDField: "b",
			rawRec:  []interface{}{"aval", uint64(7)},
			rowID:   uint64(7),
			rowVals: []interface{}{"aval"},
			err:     "field type mutex is not currently supported through Batch",
		},
		{
			name:     "string array field",
			schema:   []pdk.Field{pdk.StringArrayField{NameVal: "a"}, pdk.StringField{NameVal: "b"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{[]string{"aval", "aval2"}, uint64(7)},
			rowID:    []byte{0, 0, 0, 0, 0, 0, 0, 7},
			rowVals:  []interface{}{[]string{"aval", "aval2"}},
			batchErr: "[]string is not currently supported.", // TODO support this in gpexp.Batch
		},
		{
			name:     "decimal field",
			schema:   []pdk.Field{pdk.StringField{NameVal: "a"}, pdk.DecimalField{NameVal: "b", Scale: 2}},
			pkFields: []string{"a"},
			rawRec:   []interface{}{"blah", uint64(321)},
			rowID:    []byte("blah"),
			rowVals:  []interface{}{int64(321)},
		},
	}

	for _, test := range tests {
		// test on fresh Pilosa
		t.Run(test.name+"-1", func(t *testing.T) {
			runTest(t, test, false)
		})
		// test again with index/fields in place
		t.Run(test.name+"-2", func(t *testing.T) {
			runTest(t, test, true)
		})
	}
}

func testErr(t *testing.T, exp string, actual error) (done bool) {
	t.Helper()
	if exp == "" && actual == nil {
		return false
	}
	if exp == "" && actual != nil {
		t.Fatalf("unexpected error: %v", actual)
	}
	if exp != "" && actual == nil {
		t.Fatalf("expected error like '%s'", exp)
	}
	if !strings.Contains(actual.Error(), exp) {
		t.Fatalf("unmatched errs exp/got\n%s\n%v", exp, actual)
	}
	return true
}
