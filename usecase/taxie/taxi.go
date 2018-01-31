package taxie

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/csv"
	"github.com/pilosa/pdk/enterprise"
	"github.com/pkg/errors"

	"net/http"
	_ "net/http/pprof"
	"runtime"
)

type Main struct {
	Pilosa  []string `help:"Pilosa cluster: comma separated list of host:port."`
	URLFile string   `help:"File containing URLs of taxi data CSVs - can be local or http urls."`
	Index   string   `help:"Pilosa index to import into."`
	BufSize uint     `help:"Buffer size for imports."`
}

func NewMain() *Main {
	return &Main{
		Pilosa:  []string{"localhost:20202"}, // enterprise default
		URLFile: "greenAndYellowUrls.txt",
		Index:   "taxie",
		BufSize: 1000,
	}
}

func (m *Main) Run() error {
	// set up profiling/debugging stuff
	runtime.SetBlockProfileRate(1000)
	runtime.SetMutexProfileFraction(1000)
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// open, read, and close the URL file.
	f, err := os.Open(m.URLFile)
	if err != nil {
		return errors.Wrap(err, "opening url file")
	}
	urls, err := getURLs(f)
	if err != nil {
		return errors.Wrap(err, "getting URLs")
	}
	err = f.Close()
	if err != nil {
		return errors.Wrap(err, "closing url file")
	}

	// set up the PDK pipeline
	src := csv.NewSource(csv.WithURLs(urls))
	parser := NewTaxiParser()
	indexer, err := enterprise.SetupIndex(m.Pilosa, m.Index, nil, m.BufSize)
	if err != nil {
		return errors.Wrap(err, "setting up index")
	}
	ingester := enterprise.NewIngester(src, parser, indexer)
	return ingester.Run()
}

func getURLs(r io.Reader) ([]string, error) {
	scan := bufio.NewScanner(r)
	urls := make([]string, 0)
	for scan.Scan() {
		urls = append(urls, scan.Text())
	}
	return urls, errors.Wrap(scan.Err(), "scanner ")
}

type TaxiParser struct {
	Frames map[string]string // map from column headers to frame
}

func NewTaxiParser() *TaxiParser {
	return &TaxiParser{
		Frames: map[string]string{
			"pickup_latitude":       "pickup_latitude",
			"start_lat":             "pickup_latitude",
			"dropoff_latitude":      "dropoff_latitude",
			"end_lat":               "dropoff_latitude",
			"pickup_longitude":      "pickup_longitude",
			"start_lon":             "pickup_longitude",
			"dropoff_longitude":     "dropoff_longitude",
			"end_lon":               "dropoff_longitude",
			"passenger_count":       "passenger_count",
			"extra":                 "extra",
			"vendor_name":           "vendor_name",
			"vendor_id":             "vendor_name",
			"vendorid":              "vendor_name",
			"store_and_fwd_flag":    "store_and_fwd_flag",
			"store_and_forward":     "store_and_fwd_flag",
			"tolls_amount":          "tolls_amount",
			"tolls_amt":             "tolls_amount",
			"total_amount":          "total_amount",
			"total_amt":             "total_amount",
			"lpep_pickup_datetime":  "pickup_datetime",
			"trip_pickup_datetime":  "pickup_datetime",
			"pickup_datetime":       "pickup_datetime",
			"tpep_pickup_datetime":  "pickup_datetime",
			"lpep_dropoff_datetime": "dropoff_datetime",
			"trip_dropoff_datetime": "dropoff_datetime",
			"dropoff_datetime":      "dropoff_datetime",
			"tpep_dropoff_datetime": "dropoff_datetime",
			"fare_amount":           "fare_amount",
			"trip_distance":         "trip_distance",
			"fare_amt":              "fare_amount",
			"mta_tax":               "mta_tax",
			"tip_amount":            "tip_amount",
			"tip_amnt":              "tip_amount",
			"ratecodeid":            "rate_code",
			"rate_code":             "rate_code",
			"payment_type":          "payment_type",
			"trip_type":             "trip_type",
			"improvement_surcharge": "surcharge",
			"surcharge":             "surcharge",
		},
	}
}

func (p *TaxiParser) Parse(data interface{}) (*pdk.Entity, error) {
	rec, ok := data.(map[string]string)
	if !ok {
		return nil, errors.Errorf("Expected map[string]string, but got %#v", data)
	}
	e := pdk.NewEntity()
	for headerRaw, val := range rec {
		var v interface{}
		var parsedVal pdk.Object
		var frame string
		var err error
		switch frame = p.getFrame(headerRaw); frame {
		case "pickup_latitude", "dropoff_latitude", "pickup_longitude", "dropoff_longitude":
			v, err = strconv.ParseFloat(val, 64)
			parsedVal = pdk.F64(v.(float64))
		case "passenger_count":
			v, err = strconv.ParseUint(val, 0, 8)
			parsedVal = pdk.U8(v.(uint64))
		case "extra", "vendor_name", "store_and_fwd_flag", "rate_code", "payment_type", "trip_type":
			parsedVal = pdk.S(val)
		case "tolls_amount", "total_amount", "fare_amount", "mta_tax", "tip_amount", "surcharge":
			v, err = strconv.ParseFloat(val, 32)
			parsedVal = pdk.U16(v.(float64) * 100)
		case "trip_distance":
			v, err = strconv.ParseFloat(val, 32)
			parsedVal = pdk.U16(v.(float64) * 10)
		case "pickup_datetime", "dropoff_datetime":
			v, err = time.Parse("2006-01-02 15:04:05", val)
			parsedVal = pdk.Time(v.(time.Time))
		case "":
			if headerRaw != "," {
				return nil, errors.Errorf("couldn't map %s to a frame", headerRaw)
			}
			e.Subject = pdk.IRI(val)
			continue
		default:
			panic(fmt.Sprintf("unhandled frame: %s", frame))
		}
		if err != nil {
			return nil, errors.Wrapf(err, "parsing %s at %s", headerRaw, val)
		}
		e.Objects[pdk.Property(frame)] = parsedVal
	}
	return e, nil
}

func (p *TaxiParser) getFrame(raw string) string {
	header := strings.ToLower(strings.TrimSpace(raw))
	return p.Frames[header]
}
