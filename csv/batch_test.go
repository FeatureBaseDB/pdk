package csv_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/pilosa/go-pilosa"
	picsv "github.com/pilosa/pdk/csv"
	"github.com/pkg/errors"
)

func BenchmarkImportCSV(b *testing.B) {
	m := picsv.NewMain()
	m.BatchSize = 1 << 20
	m.Index = "picsvbench"
	m.Files = []string{"testdata/marketing-200k.csv"}
	getRawData(b, m.Files[0])
	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		b.Fatalf("getting client: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := m.Run()
		if err != nil {
			b.Fatalf("running import: %v", err)
		}
		b.StopTimer()
		err = client.DeleteIndexByName(m.Index)
		if err != nil {
			b.Fatalf("deleting index: %v", err)
		}
		b.StartTimer()
	}

}

func getRawData(t testing.TB, file string) {
	if _, err := os.Open(file); err == nil {
		return
	} else if !os.IsNotExist(err) {
		t.Fatalf("opening %s: %v", file, err)
	}
	// if the file doesn't exist
	f, err := os.Create(file)
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	resp, err := http.Get(fmt.Sprintf("https://molecula-sample-data.s3.amazonaws.com/%s", file))
	if err != nil {
		t.Fatalf("getting data: %v", err)
	}
	if resp.StatusCode > 299 {
		t.Fatalf("getting data failed: %v", resp.Status)
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		t.Fatalf("copying data into file: %v", err)
	}

	err = f.Close()
	if err != nil {
		t.Fatalf("closing file: %v", err)
	}

}

func TestImportMarketingCSV(t *testing.T) {
	cases := []struct {
		name    string
		idField string
		idType  string
	}{
		{
			name:    "stringID",
			idField: "id",
			idType:  "string",
		},
		{
			name:    "uint64",
			idField: "id",
			idType:  "string",
		},
		{
			name:    "generatedID",
			idField: "",
			idType:  "",
		},
	}
	for _, tst := range cases {
		t.Run(tst.name, func(t *testing.T) {
			m := picsv.NewMain()
			m.BatchSize = 99999
			m.Index = "testpicsv"
			m.Files = []string{"marketing-200k.csv"}
			m.Config.SourceFields["age"] = picsv.SourceField{TargetField: "age", Type: "float"}
			m.Config.PilosaFields["age"] = picsv.Field{Type: "int"}
			m.Config.IDField = tst.idField
			m.Config.IDType = tst.idType
			getRawData(t, m.Files[0])
			client, err := pilosa.NewClient(m.Pilosa)
			if err != nil {
				t.Fatalf("getting client: %v", err)
			}

			defer func() {
				err = client.DeleteIndexByName(m.Index)
				if err != nil {
					t.Fatalf("deleting index: %v", err)
				}
			}()
			err = m.Run()
			if err != nil {
				t.Fatalf("running ingest: %v", err)
			}

			schema, err := client.Schema()
			if err != nil {
				t.Fatalf("getting schema: %v", err)
			}

			index := schema.Index(m.Index)
			marital := index.Field("marital")
			converted := index.Field("converted")
			age := index.Field("age")
			education := index.Field("education")

			tests := []struct {
				query *pilosa.PQLRowQuery
				bash  string
				exp   int64
			}{
				{
					query: marital.Row("married"),
					bash:  `awk -F, '/married/ {print $1,$4}' marketing-200k.csv | sort | uniq | wc`,
					exp:   125514,
				},
				{
					query: converted.Row("no"),
					bash:  `awk -F, '{print $1,$17}' marketing-200k.csv | grep "no" |sort | uniq | wc`,
					exp:   199999,
				},
				{
					query: age.Equals(55),
					bash:  `awk -F, '{print $1,$2}' marketing-200k.csv | grep " 55.0" |sort | uniq | wc`,
					exp:   3282,
				},
				{
					query: education.Row("professional course"),
					bash:  `awk -F, '/professional course/ {print $1,$5}' marketing-200k.csv | sort | uniq | wc`,
					exp:   25374,
				},
			}

			for i, test := range tests {
				t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
					q := index.Count(test.query)
					resp, err := client.Query(q)
					if err != nil {
						t.Fatalf("running query '%s': %v", q.Serialize(), err)
					}
					if resp.Result().Count() != test.exp {
						t.Fatalf("Got unexpected result %d instead of %d for\nquery: %s\nbash: %s", resp.Result().Count(), test.exp, q.Serialize(), test.bash)
					}
				})
			}

		})
	}
}

func TestImportMultipleTaxi(t *testing.T) {
	// for url in `grep -v fhv_tripdata ../usecase/taxi/urls.txt`; do curl -s $url | head > testdata/${url##*/}; done
	m := picsv.NewMain()
	m.BatchSize = 12
	m.Index = "testdtaxi"
	m.Files = getFiles(t, "./testdata/taxi/")
	m.ConfigFile = "./testdata/taxiconfig.json"
	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	defer func() {
		err = client.DeleteIndexByName(m.Index)
		if err != nil {
			t.Logf("deleting index: %v", err)
		}
	}()

	config := `{
"pilosa-fields": {
    "cab_type": {"type": "set", "keys": true, "cache-type": "ranked", "cache-size": 3},
    "pickup_time": {"type": "int"},
    "dropoff_time": {"type": "int"},
    "passenger_count": {"type": "set", "keys": false, "cache-type": "ranked", "cache-size": 50},
    "trip_distance": {"type": "int"},
    "pickup_longitude": {"type": "int"},
    "pickup_latitude": {"type": "int"},
    "dropoff_longitude": {"type": "int"},
    "dropoff_latitude": {"type": "int"},
    "store_and_fwd_flag": {"type": "set", "keys": true},
    "rate_code": {"type": "set", "keys": true},
    "fare_amount": {"type": "int"},
    "extra": {"type": "int"},
    "mta_tax": {"type": "int"},
    "tip_amount": {"type": "int"},
    "tolls_amount": {"type": "int"},
    "total_amount": {"type": "int"},
    "improvement_surcharge": {"type": "int"},
    "ehail_fee": {"type": "int"},
    "payment_type": {"type": "set", "keys": true}
    },
"id-field": "",
"id-type": "",
"source-fields": {
        "VendorID": {"target-field": "cab_type", "type": "string"},
        "vendor_id": {"target-field": "cab_type", "type": "string"},
        "vendor_name": {"target-field": "cab_type", "type": "string"},
        "lpep_pickup_datetime": {"target-field": "pickup_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "tpep_pickup_datetime": {"target-field": "pickup_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "pickup_datetime": {"target-field": "pickup_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "Trip_Pickup_Datetime": {"target-field": "pickup_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "Lpep_dropoff_datetime": {"target-field": "dropoff_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "tpep_dropoff_datetime": {"target-field": "dropoff_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "dropoff_datetime": {"target-field": "dropoff_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "Trip_Dropoff_Datetime": {"target-field": "dropoff_time", "type": "time", "time-format": "2006-01-02 15:04:05"},
        "passenger_count": {"target-field": "passenger_count", "type": "rowID"},
        "Passenger_count": {"target-field": "passenger_count", "type": "rowID"},
        "Passenger_Count": {"target-field": "passenger_count", "type": "rowID"},
        "trip_distance": {"target-field": "trip_distance", "type": "float", "multiplier": 100},
        "Trip_distance": {"target-field": "trip_distance", "type": "float", "multiplier": 100},
        "trip_Distance": {"target-field": "trip_distance", "type": "float", "multiplier": 100},
        "pickup_longitude": {"target-field": "pickup_longitude", "type": "float", "multiplier": 10000},
        "Pickup_longitude": {"target-field": "pickup_longitude", "type": "float", "multiplier": 10000},
        "dropoff_longitude": {"target-field": "dropoff_longitude", "type": "float", "multiplier": 10000},
        "Dropoff_longitude": {"target-field": "dropoff_longitude", "type": "float", "multiplier": 10000},
        "pickup_latitude": {"target-field": "pickup_latitude", "type": "float", "multiplier": 10000},
        "Pickup_latitude": {"target-field": "pickup_latitude", "type": "float", "multiplier": 10000},
        "dropoff_latitude": {"target-field": "dropoff_latitude", "type": "float", "multiplier": 10000},
        "Dropoff_latitude": {"target-field": "dropoff_latitude", "type": "float", "multiplier": 10000},
        "store_and_fwd_flag": {"target-field": "store_and_fwd_flag", "type": "string"},
        "Store_and_fwd_flag": {"target-field": "store_and_fwd_flag", "type": "string"},
        "store_and_fwd": {"target-field": "store_and_fwd_flag", "type": "string"},
        "rate_code": {"target-field": "rate_code", "type": "string"},
        "Rate_Code": {"target-field": "rate_code", "type": "string"},
        "RateCodeID": {"target-field": "rate_code", "type": "string"},
        "Fare_Amt": {"target-field": "fare_amount", "type": "float", "multiplier": 100},
        "fare_amount": {"target-field": "fare_amount", "type": "float", "multiplier": 100},
        "Tip_Amt": {"target-field": "tip_amount", "type": "float", "multiplier": 100},
        "tip_amount": {"target-field": "tip_amount", "type": "float", "multiplier": 100},
        "Tolls_Amt": {"target-field": "tolls_amount", "type": "float", "multiplier": 100},
        "tolls_amount": {"target-field": "tolls_amount", "type": "float", "multiplier": 100},
        "improvement_surcharge": {"target-field": "improvement_surcharge", "type": "float", "multiplier": 100},
        "surcharge": {"target-field": "improvement_surcharge", "type": "float", "multiplier": 100},
        "mta_tax": {"target-field": "mta_tax", "type": "float", "multiplier": 100},
        "Total_Amt": {"target-field": "total_amount", "type": "float", "multiplier": 100},
        "total_amount": {"target-field": "total_amount", "type": "float", "multiplier": 100},
        "ehail_fee": {"target-field": "ehail_fee", "type": "float", "multiplier": 100},
        "payment_type": {"target-field": "payment_type", "type": "string"},
        "extra": {"target-field": "extra", "type": "float", "multiplier": 100}
    }
}`

	writeFile(t, m.ConfigFile, config)

	err = m.Run()
	if err != nil {
		t.Fatalf("running ingest: %v", err)
	}

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	index := schema.Index(m.Index)
	cabType := index.Field("cab_type")
	drop_long := index.Field("dropoff_longitude")
	pick_long := index.Field("pickup_longitude")

	tests := []struct {
		query *pilosa.PQLRowQuery
		bash  string
		exp   int64
	}{
		{
			query: cabType.Row("1"),
			bash:  `cat ./testdata/taxi/* | awk -F, '{print $1}' | sort | uniq -c`,
			exp:   79,
		},
		{
			query: cabType.Row("2"),
			bash:  `cat ./testdata/taxi/* | awk -F, '{print $1}' | sort | uniq -c`,
			exp:   363,
		},
		{
			query: cabType.Row("CMT"),
			bash:  `cat ./testdata/taxi/* | awk -F, '{print $1}' | sort | uniq -c`,
			exp:   318,
		},
		{
			query: cabType.Row("DDS"),
			bash:  `cat ./testdata/taxi/* | awk -F, '{print $1}' | sort | uniq -c`,
			exp:   17,
		},
		{
			query: cabType.Row("VTS"),
			bash:  `cat ./testdata/taxi/* | awk -F, '{print $1}' | sort | uniq -c`,
			exp:   249,
		},
		{
			query: drop_long.Equals(-738996),
			bash:  `cat * | grep '73\.8996'`,
			exp:   1,
		},
		{
			query: drop_long.Equals(-738996),
			bash:  `cat * | grep '73\.8996'`,
			exp:   1,
		},
		{
			query: index.Union(drop_long.Between(-739449, -739440), pick_long.Between(-739449, -739440)),
			bash:  `cat * | grep '73\.944' | wc`,
			exp:   16,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			q := index.Count(test.query)
			resp, err := client.Query(q)
			if err != nil {
				t.Fatalf("running query '%s': %v", q.Serialize(), err)
			}
			if resp.Result().Count() != test.exp {
				t.Fatalf("Got unexpected result %d instead of %d for\nquery: %s\nbash: %s", resp.Result().Count(), test.exp, q.Serialize(), test.bash)
			}
		})
	}

}

func TestSmallImport(t *testing.T) {
	m := picsv.NewMain()
	m.BatchSize = 1 << 20
	m.Index = "testsample"
	m.Files = []string{"testdata/sample.csv"}
	m.ConfigFile = "config.json"
	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	defer func() {
		err = client.DeleteIndexByName(m.Index)
		if err != nil {
			t.Logf("deleting index: %v", err)
		}
	}()
	config := `{
"pilosa-fields": {"size": {"type": "set", "keys": true, "cache-type": "ranked", "cache-size": 100000},
                  "age": {"type": "int"},
                  "color": {"type": "set", "keys": true},
                  "result": {"type": "int"},
                  "dayofweek": {"type": "set", "keys": false, "cache-type": "ranked", "cache-size": 7}
    },
"id-field": "ID",
"id-type": "string",
"source-fields": {
        "Size": {"target-field": "size", "type": "string"},
        "Color": {"target-field": "color", "type": "string"},
        "Age": {"target-field": "age", "type": "int"},
        "Result": {"target-field": "result", "type": "float", "multiplier": 100000000},
        "dayofweek": {"target-field": "dayofweek", "type": "uint64"}
    }
}
`
	data := `
ID,Size,Color,Age,Result,dayofweek
ABDJ,small,green,42,1.13106317,1
HFZP,large,red,99,30.23959735,2
HFZP,small,green,99,NA,3
EJSK,medium,purple,22,20.23959735,1
EJSK,large,green,35,25.13106317,
FEFF,,,,,6
`
	writeFile(t, m.ConfigFile, config)
	writeFile(t, m.Files[0], data)

	err = m.Run()
	if err != nil {
		t.Fatalf("running ingest: %v", err)
	}

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	index := schema.Index(m.Index)
	size := index.Field("size")
	color := index.Field("color")
	age := index.Field("age")
	result := index.Field("result")
	day := index.Field("dayofweek")

	tests := []struct {
		query   pilosa.PQLQuery
		resType string
		exp     interface{}
	}{
		{
			query:   index.Count(size.Row("small")),
			resType: "count",
			exp:     int64(2),
		},
		{
			query:   size.Row("small"),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "HFZP"},
		},
		{
			query:   color.Row("green"),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "HFZP", "EJSK"},
		},
		{
			query:   age.Equals(99),
			resType: "rowKeys",
			exp:     []string{"HFZP"},
		},
		{
			query:   age.GT(0),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "HFZP", "EJSK"},
		},
		{
			query:   result.GT(0),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "EJSK"},
		},
		{
			query:   result.GT(100000),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "EJSK"},
		},
		{
			query:   day.Row(1),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "EJSK"},
		},
		{
			query:   day.Row(6),
			resType: "rowKeys",
			exp:     []string{"FEFF"},
		},
		{
			query:   index.Count(day.Row(3)),
			resType: "count",
			exp:     int64(1),
		},
		{
			query:   index.Count(day.Row(2)),
			resType: "count",
			exp:     int64(1), // not mutually exclusive!
		},
		{
			query:   size.Row(`""`), // TODO... go-pilosa should probably serialize keys into PQL using quotes.
			resType: "rowKeys",
			exp:     []string{}, // empty strings are ignored rather than ingested
		},
		{
			query:   color.Row(`""`),
			resType: "rowKeys",
			exp:     []string{}, // empty strings are ignored rather than ingested
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := client.Query(test.query)
			if err != nil {
				t.Fatalf("running query: %v", err)
			}
			res := resp.Result()
			switch test.resType {
			case "count":
				if res.Count() != test.exp.(int64) {
					t.Fatalf("unexpected count %d is not %d", res.Count(), test.exp.(int64))
				}
			case "rowKeys":
				got := res.Row().Keys
				exp := test.exp.([]string)
				if err := isPermutationOf(got, exp); err != nil {
					t.Fatalf("unequal rows %v expected/got:\n%v\n%v", err, exp, got)
				}
			}
		})
	}

}

func writeFile(t testing.TB, name, contents string) {
	cf, err := os.Create(name)
	if err != nil {
		t.Fatalf("creating config file: %v", err)
	}
	_, err = cf.Write([]byte(contents))
	if err != nil {
		t.Fatalf("writing config file: %v", err)
	}
}

func isPermutationOf(one, two []string) error {
	if len(one) != len(two) {
		return errors.Errorf("different lengths %d and %d", len(one), len(two))
	}
outer:
	for _, vOne := range one {
		for j, vTwo := range two {
			if vOne == vTwo {
				two = append(two[:j], two[j+1:]...)
				continue outer
			}
		}
		return errors.Errorf("%s in one but not two", vOne)
	}
	if len(two) != 0 {
		return errors.Errorf("vals in two but not one: %v", two)
	}
	return nil
}

func getFiles(t testing.TB, dir string) []string {
	f, err := os.Open(dir)
	if err != nil {
		t.Fatalf("opening %s: %v", dir, err)
	}
	fis, err := f.Readdirnames(0)
	if err != nil {
		t.Fatalf(": %v", err)
	}

	for i, name := range fis {
		fis[i] = filepath.Join(dir, name)
	}

	return fis
}
