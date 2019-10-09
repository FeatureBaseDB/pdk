package pdk

import (
	"reflect"
	"strings"
	"testing"

	"github.com/pilosa/go-pilosa/gpexp"
)

func TestGetPrimaryKeyRecordizer(t *testing.T) {
	tests := []struct {
		name     string
		schema   []Field
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
			schema: []Field{StringField{}},
			expErr: "can't call getPrimaryKeyRecordizer with empty pkFields",
		},
		{
			name:     "primary is StringArray",
			schema:   []Field{StringArrayField{NameVal: "blah"}},
			pkFields: []string{"blah"},
			expErr:   "field blah cannot be a primary key field because it is a StringArray field.",
		},
		{
			name:     "primary is StringArray complex",
			schema:   []Field{StringField{NameVal: "zaa"}, IntField{NameVal: "hey"}, StringArrayField{NameVal: "blah"}},
			pkFields: []string{"blah", "zaa"},
			expErr:   "field blah cannot be a primary key field because it is a StringArray field.",
		},
		{
			name:     "unknown pkfield",
			schema:   []Field{StringField{NameVal: "zaa"}},
			pkFields: []string{"zaa", "zz"},
			expErr:   "no field with primary key field name zz found",
		},
		{
			name:     "unknown pkfield complex",
			schema:   []Field{StringField{NameVal: "zaa"}, IntField{NameVal: "hey"}, StringField{NameVal: "blah"}},
			pkFields: []string{"blah", "zz", "zaa"},
			expErr:   "no field with primary key field name zz found",
		},
		{
			name:     "skip primary",
			schema:   []Field{StringField{NameVal: "a"}, IntField{NameVal: "b"}},
			pkFields: []string{"a"},
			expSkip:  map[int]struct{}{0: struct{}{}},
			rawRec:   []interface{}{"a", 9},
			expID:    []byte("a"),
		},
		{
			name:     "primaries as ints",
			schema:   []Field{StringField{NameVal: "a"}, IntField{NameVal: "b"}, IntField{NameVal: "c"}, IntField{NameVal: "d"}},
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
		schema    []Field
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
		m.NewSource = func() (Source, error) { return nil, nil }

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
			schema:  []Field{StringField{}},
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
			schema:    []Field{BoolField{NameVal: "a"}, IDField{NameVal: "b"}, BoolField{NameVal: "c"}},
			IDField:   "b",
			packBools: "bff",
			rawRec:    []interface{}{true, uint64(7), false},
			rowID:     uint64(7),
			rowVals:   []interface{}{"a", "a", nil, "c"},
		},
		{
			name:    "don't pack bools",
			schema:  []Field{BoolField{NameVal: "a"}, IDField{NameVal: "b"}, BoolField{NameVal: "c"}},
			IDField: "b",
			rawRec:  []interface{}{true, uint64(7), false},
			rowID:   uint64(7),
			rowVals: []interface{}{true, false},
			err:     "field type bool is not currently supported through Batch",
		},
		{
			name:    "mutex field",
			schema:  []Field{StringField{NameVal: "a", Mutex: true}, IDField{NameVal: "b"}},
			IDField: "b",
			rawRec:  []interface{}{"aval", uint64(7)},
			rowID:   uint64(7),
			rowVals: []interface{}{"aval"},
			err:     "field type mutex is not currently supported through Batch",
		},
		{
			name:     "string array field",
			schema:   []Field{StringArrayField{NameVal: "a"}, StringField{NameVal: "b"}},
			pkFields: []string{"b"},
			rawRec:   []interface{}{[]string{"aval", "aval2"}, uint64(7)},
			rowID:    []byte{0, 0, 0, 0, 0, 0, 0, 7},
			rowVals:  []interface{}{[]string{"aval", "aval2"}},
		},
		{
			name:     "decimal field",
			schema:   []Field{StringField{NameVal: "a"}, DecimalField{NameVal: "b", Scale: 2}},
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
