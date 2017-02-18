package taxi

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa"
)

/**************
common tools
**************/

// QueryBuilder maintains a query string for a single profileID
type QueryBuilder struct {
	profileID uint64
	query     string
}

// Add appends a SetBit() query to the query string
func (qb *QueryBuilder) Add(bitmapID uint64, frame string) {
	qb.query += fmt.Sprintf("SetBit(id=%d, frame=%s, profileID=%d)", bitmapID, frame, qb.profileID)
}

// Query returns the full query string
func (qb *QueryBuilder) Query() string { return qb.query }

// Nexter generates unique bitmapIDs
type Nexter struct {
	id   uint64
	lock sync.Mutex
}

// Next generates a new bitmapID
func (n *Nexter) Next() (nextID uint64) {
	n.lock.Lock()
	nextID = n.id
	n.id++
	n.lock.Unlock()
	return
}

/***************
use case setup
***************/
const (
	// for field meanings, see http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
	VendorID = iota
	Lpep_pickup_datetime
	Lpep_dropoff_datetime
	Store_and_fwd_flag
	RateCodeID
	Pickup_longitude
	Pickup_latitude
	Dropoff_longitude
	Dropoff_latitude
	Passenger_count
	Trip_distance
	Fare_amount
	Extra
	MTA_tax
	Tip_amount
	Tolls_amount
	Ehail_fee
	Total_amount
	Payment_type
	Trip_type
)

var layout = "2006-01-02 15:04:05"

/***********************
use case implementation
***********************/

// TODO autoscan 1. determine field type by attempting conversions
// TODO autoscan 2. determine field mapping by looking at statistics (for floatmapper, intmapper)
// TODO autoscan 3. write results from ^^ to config file
// TODO read ParserMapper config from file (cant do CustomMapper)

type Main struct {
	UrlFile     string
	Concurrency int
	Database    string

	urls []string
	bms  []pdk.BitMapper
	ams  []pdk.AttrMapper

	nexter *Nexter
}

func NewMain() *Main {
	m := &Main{
		Concurrency: 1,
		nexter:      &Nexter{},
	}
	// TODO read file
	m.urls = []string{
		"https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv",
	}
	return m
}

func (m *Main) Run() {

	urls := make(chan string)
	records := make(chan string)

	go func() {
		for _, url := range m.urls {
			urls <- url
		}
		close(urls)
	}()

	m.bms = getBitMappers()
	m.ams = getAttrMappers()

	var wg sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg.Add(1)
		go func() {
			m.fetch(urls, records)
			wg.Done()
		}()
	}
	var wg2 sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg2.Add(1)
		go func() {
			m.parseMapAndPost(records)
			wg2.Done()
		}()
	}
	wg.Wait()
	close(records)
	wg2.Wait()
}

func (m *Main) fetch(urls <-chan string, records chan<- string) {
	for url := range urls {
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("fetching %s, err: %v", url, err)
			continue
		}

		scan := bufio.NewScanner(resp.Body)
		for scan.Scan() {
			record := scan.Text()
			records <- record
		}
	}
}

//		mapRecordToBitmaps(record, profileID, m.bitMappers)
//      mapRecordToAttrs(record, profileID, m.attrMappers)

func (m *Main) parseMapAndPost(records <-chan string) {
	for record := range records {
		profileID := m.nexter.Next()
		client, err := pilosa.NewClient("localhost:15000")
		if err != nil {
			panic(err)
		}
		qb := &QueryBuilder{}
		qb.profileID = profileID
		qb.query = ""

		fields := strings.Split(record, ",")
		for _, pm := range m.bms {
			if len(pm.Fields) != len(pm.Parsers) {
				// TODO if len(pm.Parsers) == 1, use that for all fields
				log.Printf("parse: BitMapper has different number of fields: %v and parsers: %v", pm.Fields, pm.Parsers)
				continue
			}

			// parse fields into a slice `parsed`
			parsed := make([]interface{}, 0, len(pm.Fields))
			skip := false
			for n, fieldnum := range pm.Fields {
				parser := pm.Parsers[n]
				if fieldnum >= len(fields) {
					log.Println("parse: field index out of range")
					skip = true
					break
				}
				parsedField, err := parser.Parse(fields[fieldnum])
				if err != nil {
					fmt.Println(err)
					skip = true
					break
				}
				parsed = append(parsed, parsedField)
			}
			if skip {
				continue
			}

			// map those fields to a slice of IDs
			ids, err := pm.Mapper.ID(parsed...)
			if err != nil {
				fmt.Println(err)
				continue
			}
			for _, id := range ids {
				qb.Add(uint64(id), pm.Frame)
			}
		}
		_, err = client.ExecuteQuery(context.Background(), m.Database, qb.Query(), true)
		if err != nil {
			fmt.Println(err)
		}

	}
}

func getAttrMappers() []pdk.AttrMapper {
	ams := []pdk.AttrMapper{}

	return ams
}

func getBitMappers() []pdk.BitMapper {
	// map a pair of floats to a grid sector of a rectangular region
	gm := pdk.GridMapper{
		Xmin: -74.27,
		Xmax: -73.69,
		Xres: 100,
		Ymin: 40.48,
		Ymax: 40.93,
		Yres: 100,
	}

	// map a float according to a custom set of bins
	// seems like the LFM defined below is more sensible
	/*
		fm := pdk.FloatMapper{
			Buckets: []float64{0, 0.5, 1, 2, 5, 10, 25, 50, 100, 200},
		}
	*/

	// this set of bins is equivalent to rounding to nearest int (TODO verify)
	lfm := pdk.LinearFloatMapper{
		Min: -0.5,
		Max: 200.5,
		Res: 201,
	}

	// map the (pickupTime, dropTime) pair, according to the duration in minutes, binned using `fm`
	durm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			return end.Sub(start).Minutes()
		},
		Mapper: lfm,
	}

	// map the (pickupTime, dropTime, dist) triple, according to the speed in mph, binned using `fm`
	speedm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			dist := fields[2].(float64)
			return dist / end.Sub(start).Hours()
		},
		Mapper: lfm,
	}

	tp := pdk.TimeParser{Layout: layout}

	bms := []pdk.BitMapper{
		pdk.BitMapper{
			Frame:   "passengerCount",
			Mapper:  pdk.IntMapper{Min: 1, Max: 8},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{Passenger_count},
		},
		pdk.BitMapper{
			Frame:   "totalAmount_dollars",
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{Total_amount},
		},
		pdk.BitMapper{
			Frame:   "cabType",
			Mapper:  pdk.IntMapper{Min: 0, Max: 2},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{VendorID},
		},
		pdk.BitMapper{
			Frame:   "pickupTime",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.BitMapper{
			Frame:   "pickupDay",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.BitMapper{
			Frame:   "pickupMonth",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.BitMapper{
			Frame:   "pickupYear",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.BitMapper{
			Frame:   "dropTime",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.BitMapper{
			Frame:   "dropDay",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.BitMapper{
			Frame:   "dropMonth",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.BitMapper{
			Frame:   "dropYear",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.BitMapper{
			Frame:   "dist_miles", // note "_miles" is a unit annotation
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{Trip_distance},
		},
		pdk.BitMapper{
			Frame:   "duration_minutes",
			Mapper:  durm,
			Parsers: []pdk.Parser{tp, tp},
			Fields:  []int{Lpep_pickup_datetime, Lpep_dropoff_datetime},
		},
		pdk.BitMapper{
			Frame:   "speed_mph",
			Mapper:  speedm,
			Parsers: []pdk.Parser{tp, tp, pdk.FloatParser{}},
			Fields:  []int{Lpep_pickup_datetime, Lpep_dropoff_datetime, Trip_distance},
		},
		pdk.BitMapper{
			Frame:   "pickupGridID",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{Pickup_longitude, Pickup_latitude},
		},
		pdk.BitMapper{
			Frame:   "dropGridID",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{Dropoff_longitude, Dropoff_latitude},
		},
	}

	return bms
}
