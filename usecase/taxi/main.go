package taxi

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pdk"
)

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

/***********************
use case implementation
***********************/

// TODO autoscan 1. determine field type by attempting conversions
// TODO autoscan 2. determine field mapping by looking at statistics (for floatmapper, intmapper)
// TODO autoscan 3. write results from ^^ to config file
// TODO read ParserMapper config from file (cant do CustomMapper)

type Main struct {
	PilosaHost  string
	URLFile     string
	Concurrency int
	Database    string

	importer pdk.PilosaImporter
	urls     []string
	bms      []pdk.BitMapper
	ams      []pdk.AttrMapper

	nexter *Nexter

	totalBytes int64
	bytesLock  sync.Mutex
}

func NewMain() *Main {
	m := &Main{
		Concurrency: 1,
		nexter:      &Nexter{},
		urls:        make([]string, 0),
	}

	return m
}

func (m *Main) Run() error {
	err := m.readURLs()
	if err != nil {
		return err
	}

	frames := []string{"passengerCount", "totalAmount_dollars", "cabType", "pickupTime", "pickupDay", "pickupMonth", "pickupYear", "dropTime", "dropDay", "dropMonth", "dropYear", "dist_miles", "duration_minutes", "speed_mph", "pickupGridID", "dropGridID"}
	m.importer = pdk.NewImportClient(m.PilosaHost, m.Database, frames)

	ticker := m.printStats()

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
	m.importer.Close()
	ticker.Stop()
	return nil
}

func (m *Main) readURLs() error {
	if m.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(m.URLFile)
	if err != nil {
		return err
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		m.urls = append(m.urls, s.Text())
	}
	if err := s.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	go func() {
		for range t.C {
			log.Printf("Profiles: %d, Bytes: %s", m.nexter.Last(), pdk.Bytes(m.BytesProcessed()))
		}
	}()
	return t
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
			m.AddBytes(len(record))
			records <- record
		}
	}
}

func (m *Main) parseMapAndPost(records <-chan string) {
	for record := range records {
		profileID := m.nexter.Next()

		fields := strings.Split(record, ",")
		for _, bm := range m.bms {
			if len(bm.Fields) != len(bm.Parsers) {
				// TODO if len(pm.Parsers) == 1, use that for all fields
				log.Printf("parse: BitMapper has different number of fields: %v and parsers: %v", bm.Fields, bm.Parsers)
				continue
			}

			// parse fields into a slice `parsed`
			parsed := make([]interface{}, 0, len(bm.Fields))
			skip := false
			for n, fieldnum := range bm.Fields {
				parser := bm.Parsers[n]
				if fieldnum >= len(fields) {
					log.Printf("parse: field index: %v out of range for: %v", fieldnum, fields)
					skip = true
					break
				}
				parsedField, err := parser.Parse(fields[fieldnum])
				if err != nil {
					skip = true
					break
				}
				parsed = append(parsed, parsedField)
			}
			if skip {
				continue
			}

			// map those fields to a slice of IDs
			ids, err := bm.Mapper.ID(parsed...)
			if err != nil {
				continue
			}
			for _, id := range ids {
				m.importer.SetBit(uint64(id), profileID, bm.Frame)
			}
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

	tp := pdk.TimeParser{Layout: "2006-01-02 15:04:05"}

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

func (m *Main) AddBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

func (m *Main) BytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

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

func (n *Nexter) Last() (lastID uint64) {
	n.lock.Lock()
	lastID = n.id - 1
	n.lock.Unlock()
	return
}
