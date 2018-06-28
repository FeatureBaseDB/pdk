// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package taxi

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	// for profiling
	_ "net/http/pprof"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

/***********************
use case implementation
***********************/

// TODO autoscan 1. determine field type by attempting conversions
// TODO autoscan 2. determine field mapping by looking at statistics (for floatmapper, intmapper)
// TODO autoscan 3. write results from ^^ to config file
// TODO read ParserMapper config from file (cant do CustomMapper)

// Main holds options and execution state for taxi usecase.
type Main struct {
	PilosaHost       string
	URLFile          string
	FetchConcurrency int
	Concurrency      int
	Index            string
	BufferSize       int
	UseReadAll       bool

	indexer   pdk.Indexer
	urls      []string
	greenBms  []pdk.ColumnMapper
	yellowBms []pdk.ColumnMapper
	ams       []pdk.AttrMapper

	nexter pdk.INexter

	totalBytes int64
	bytesLock  sync.Mutex

	totalRecs     *counter
	skippedRecs   *counter
	nullLocs      *counter
	badLocs       *counter
	badSpeeds     *counter
	badTotalAmnts *counter
	badDurations  *counter
	badPassCounts *counter
	badDist       *counter
	badUnknowns   *counter
}

// NewMain returns a new instance of Main with default values.
func NewMain() *Main {
	m := &Main{
		Concurrency:      1,
		FetchConcurrency: 1,
		Index:            "taxi",
		nexter:           pdk.NewNexter(),
		urls:             make([]string, 0),

		totalRecs:     &counter{},
		skippedRecs:   &counter{},
		nullLocs:      &counter{},
		badLocs:       &counter{},
		badSpeeds:     &counter{},
		badTotalAmnts: &counter{},
		badDurations:  &counter{},
		badPassCounts: &counter{},
		badDist:       &counter{},
		badUnknowns:   &counter{},
	}

	return m
}

// Run runs the taxi usecase.
func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := m.readURLs()
	if err != nil {
		return err
	}

	schema := gopilosa.NewSchema()
	index, err := schema.Index(m.Index)
	if err != nil {
		return errors.Wrap(err, "describing index")
	}

	pdk.NewRankedField(index, "cab_type", 10)
	pdk.NewRankedField(index, "passenger_count", 1000)
	pdk.NewRankedField(index, "total_amount_dollars", 10000)
	pdk.NewRankedField(index, "pickup_time", 10000)
	pdk.NewRankedField(index, "pickup_day", 10000)
	pdk.NewRankedField(index, "pickup_mday", 10000)
	pdk.NewRankedField(index, "pickup_month", 13)
	pdk.NewRankedField(index, "pickup_year", 100)
	pdk.NewRankedField(index, "drop_time", 10000)
	pdk.NewRankedField(index, "drop_day", 10000)
	pdk.NewRankedField(index, "drop_mday", 10000)
	pdk.NewRankedField(index, "drop_month", 13)
	pdk.NewRankedField(index, "drop_year", 1000)
	pdk.NewRankedField(index, "dist_miles", 1000)
	pdk.NewRankedField(index, "duration_minutes", 10000)
	pdk.NewRankedField(index, "speed_mph", 10000)
	pdk.NewRankedField(index, "pickup_grid_id", 10000)
	pdk.NewRankedField(index, "drop_grid_id", 10000)
	pdk.NewRankedField(index, "pickup_elevation", 10000)
	pdk.NewRankedField(index, "drop_elevation", 10000)

	m.indexer, err = pdk.SetupPilosa([]string{m.PilosaHost}, m.Index, schema, uint(m.BufferSize))
	if err != nil {
		return errors.Wrap(err, "setting up indexer")
	}

	ticker := m.printStats()

	urls := make(chan string, 100)
	records := make(chan record, 10000)

	go func() {
		for _, url := range m.urls {
			urls <- url
		}
		close(urls)
	}()

	m.greenBms = getBitMappers(greenFields)
	m.yellowBms = getBitMappers(yellowFields)
	m.ams = getAttrMappers()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Rides: %d, Bytes: %s", m.nexter.Last(), pdk.Bytes(m.bytesProcessed()))
			os.Exit(0)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < m.FetchConcurrency; i++ {
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
	err = m.indexer.Close()
	ticker.Stop()
	return errors.Wrap(err, "closing indexer")
}

func (m *Main) readURLs() error {
	if m.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(m.URLFile)
	if err != nil {
		return errors.Wrap(err, "opening url file")
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		m.urls = append(m.urls, s.Text())
	}
	err = s.Err()
	return errors.Wrap(err, "scanning url file")
}

func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.bytesProcessed()
			log.Printf("Rides: %d, Bytes: %s, Records: %v, Duration: %v, Rate: %v/s", m.nexter.Last(), pdk.Bytes(bytes), m.totalRecs.Get(), duration, pdk.Bytes(float64(bytes)/duration.Seconds()))
			log.Printf("Skipped: %v, badLocs: %v, nullLocs: %v, badSpeeds: %v, badTotalAmnts: %v, badDurations: %v, badUnknowns: %v, badPassCounts: %v, badDist: %v", m.skippedRecs.Get(), m.badLocs.Get(), m.nullLocs.Get(), m.badSpeeds.Get(), m.badTotalAmnts.Get(), m.badDurations.Get(), m.badUnknowns.Get(), m.badPassCounts.Get(), m.badDist.Get())
		}
	}()
	return t
}

// getNextURL fetches the next url from the channel, or if it is emtpy, gets a
// url from the failedURLs map after 10 seconds of waiting on the channel. As
// long as it gets a url, its boolean return value is true - if it does not get
// a url, it returns false.
func getNextURL(urls <-chan string, failedURLs map[string]int) (string, bool) {
	url, open := <-urls
	if !open {
		for url := range failedURLs {
			return url, true
		}
		return "", false
	}
	return url, true
}

func (m *Main) fetch(urls <-chan string, records chan<- record) {
	failedURLs := make(map[string]int)
	for {
		url, ok := getNextURL(urls, failedURLs)
		if !ok {
			break
		}
		var typ rune
		if strings.Contains(url, "green") {
			typ = 'g'
		} else if strings.Contains(url, "yellow") {
			typ = 'y'
		} else {
			typ = 'x'
		}
		var content io.ReadCloser
		if strings.HasPrefix(url, "http") {
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("fetching %s, err: %v", url, err)
				continue
			}
			content = resp.Body
		} else {
			f, err := os.Open(url)
			if err != nil {
				log.Printf("opening %s, err: %v", url, err)
				continue
			}
			content = f
		}
		var scan *bufio.Scanner
		if m.UseReadAll {
			// we're using ReadAll here to ensure that we can read the entire
			// file/url before we start putting it into Pilosa. Not great for memory
			// usage or smooth performance, but we want to ensure repeatable results
			// in the simplest way possible.
			contentBytes, err := ioutil.ReadAll(content)
			if err != nil {
				failedURLs[url]++
				if failedURLs[url] > 10 {
					log.Fatalf("Unrecoverable failure while fetching url: %v, err: %v. Could not read fully after 10 tries.", url, err)
				}
				continue
			}
			err = content.Close()
			if err != nil {
				log.Printf("closing %s, err: %v", url, err)
			}

			buf := bytes.NewBuffer(contentBytes)
			scan = bufio.NewScanner(buf)
		} else {
			scan = bufio.NewScanner(content)
		}

		// discard header line
		correctLine := false
		if scan.Scan() {
			header := scan.Text()
			if strings.HasPrefix(header, "vendor_") {
				correctLine = true
			}
		}
		for scan.Scan() {
			m.totalRecs.Add(1)
			rec := scan.Text()
			m.addBytes(len(rec))
			if correctLine {
				// last field needs to be shifted over by 1
				lastcomma := strings.LastIndex(rec, ",")
				if lastcomma == -1 {
					m.skippedRecs.Add(1)
					continue
				}
				rec = rec[:lastcomma] + "," + rec[lastcomma:]
			}
			records <- record{Val: rec, Type: typ}
		}
		err := scan.Err()
		if err != nil {
			log.Printf("scan error on %s, err: %v", url, err)
		}
		delete(failedURLs, url)
	}
}

type record struct {
	Type rune
	Val  string
}

func (r record) clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

type columnField struct {
	Column uint64
	Field  string
}

func (m *Main) parseMapAndPost(records <-chan record) {
Records:
	for record := range records {
		fields, ok := record.clean()
		if !ok {
			m.skippedRecs.Add(1)
			continue
		}
		var bms []pdk.ColumnMapper
		var cabType uint64
		if record.Type == 'g' {
			bms = m.greenBms
			cabType = 0
		} else if record.Type == 'y' {
			bms = m.yellowBms
			cabType = 1
		} else {
			log.Println("unknown record type")
			m.badUnknowns.Add(1)
			m.skippedRecs.Add(1)
			continue
		}
		columnsToSet := make([]columnField, 0)
		columnsToSet = append(columnsToSet, columnField{Column: cabType, Field: "cab_type"})
		for _, bm := range bms {
			if len(bm.Fields) != len(bm.Parsers) {
				// TODO if len(pm.Parsers) == 1, use that for all fields
				log.Fatalf("parse: BitMapper has different number of fields: %v and parsers: %v", bm.Fields, bm.Parsers)
			}

			// parse fields into a slice `parsed`
			parsed := make([]interface{}, 0, len(bm.Fields))
			for n, fieldnum := range bm.Fields {
				parser := bm.Parsers[n]
				if fieldnum >= len(fields) {
					log.Printf("parse: field index: %v out of range for: %v", fieldnum, fields)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsedField, err := parser.Parse(fields[fieldnum])
				if err != nil && fields[fieldnum] == "" {
					m.skippedRecs.Add(1)
					continue Records
				} else if err != nil {
					log.Printf("parsing: field: %v err: %v bm: %v rec: %v", fields[fieldnum], err, bm, record)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsed = append(parsed, parsedField)
			}

			// map those fields to a slice of IDs
			ids, err := bm.Mapper.ID(parsed...)
			if err != nil {
				if err.Error() == "point (0, 0) out of range" {
					m.nullLocs.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if strings.Contains(bm.Field, "grid_id") && strings.Contains(err.Error(), "out of range") {
					m.badLocs.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "speed_mph" && strings.Contains(err.Error(), "out of range") {
					m.badSpeeds.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "total_amount_dollars" && strings.Contains(err.Error(), "out of range") {
					m.badTotalAmnts.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "duration_minutes" && strings.Contains(err.Error(), "out of range") {
					m.badDurations.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "passenger_count" && strings.Contains(err.Error(), "out of range") {
					m.badPassCounts.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "dist_miles" && strings.Contains(err.Error(), "out of range") {
					m.badDist.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				log.Printf("mapping: bm: %v, err: %v rec: %v", bm, err, record)
				m.skippedRecs.Add(1)
				m.badUnknowns.Add(1)
				continue Records
			}
			for _, id := range ids {
				columnsToSet = append(columnsToSet, columnField{Column: uint64(id), Field: bm.Field})
			}
		}
		columnID := m.nexter.Next()
		for _, bit := range columnsToSet {
			m.indexer.AddColumn(bit.Field, columnID, bit.Column)
		}
	}
}

func getAttrMappers() []pdk.AttrMapper {
	ams := []pdk.AttrMapper{}

	return ams
}

func getBitMappers(fields map[string]int) []pdk.ColumnMapper {
	// map a pair of floats to a grid sector of a rectangular region
	gm := pdk.GridMapper{
		Xmin: -74.27,
		Xmax: -73.69,
		Xres: 100,
		Ymin: 40.48,
		Ymax: 40.93,
		Yres: 100,
	}

	elevFloatMapper := pdk.LinearFloatMapper{
		Min: -32,
		Max: 195,
		Res: 46,
	}

	gfm := pdk.NewGridToFloatMapper(gm, elevFloatMapper, elevations)

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
		Max: 3600.5,
		Res: 3601,
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

	bms := []pdk.ColumnMapper{
		pdk.ColumnMapper{
			Field:   "passenger_count",
			Mapper:  pdk.IntMapper{Min: 0, Max: 9},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{fields["passenger_count"]},
		},
		pdk.ColumnMapper{
			Field:   "total_amount_dollars",
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{fields["total_amount"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_time",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_mday",
			Mapper:  pdk.DayOfMonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_month",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_year",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_time",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_mday",
			Mapper:  pdk.DayOfMonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_month",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_year",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "dist_miles", // note "_miles" is a unit annotation
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{fields["trip_distance"]},
		},
		pdk.ColumnMapper{
			Field:   "duration_minutes",
			Mapper:  durm,
			Parsers: []pdk.Parser{tp, tp},
			Fields:  []int{fields["pickup_datetime"], fields["dropoff_datetime"]},
		},
		pdk.ColumnMapper{
			Field:   "speed_mph",
			Mapper:  speedm,
			Parsers: []pdk.Parser{tp, tp, pdk.FloatParser{}},
			Fields:  []int{fields["pickup_datetime"], fields["dropoff_datetime"], fields["trip_distance"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_grid_id",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["pickup_longitude"], fields["pickup_latitude"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_grid_id",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
		pdk.ColumnMapper{
			Field:   "pickup_elevation",
			Mapper:  gfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
		pdk.ColumnMapper{
			Field:   "drop_elevation",
			Mapper:  gfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
	}

	return bms
}

func (m *Main) addBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

func (m *Main) bytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

type counter struct {
	num  int64
	lock sync.Mutex
}

func (c *counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

func (c *counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}

/***************
use case setup
***************/
var greenFields = map[string]int{
	"vendor_id":          0,
	"pickup_datetime":    1,
	"dropoff_datetime":   2,
	"passenger_count":    9,
	"trip_distance":      10,
	"pickup_longitude":   5,
	"pickup_latitude":    6,
	"ratecode_id":        4,
	"store_and_fwd_flag": 3,
	"dropoff_longitude":  7,
	"dropoff_latitude":   8,
	"payment_type":       18,
	"fare_amount":        11,
	"extra":              12,
	"mta_tax":            13,
	"tip_amount":         14,
	"tolls_amount":       15,
	"total_amount":       17,
}

var yellowFields = map[string]int{
	"vendor_id":             0,
	"pickup_datetime":       1,
	"dropoff_datetime":      2,
	"passenger_count":       3,
	"trip_distance":         4,
	"pickup_longitude":      5,
	"pickup_latitude":       6,
	"ratecode_id":           7,
	"store_and_fwd_flag":    8,
	"dropoff_longitude":     9,
	"dropoff_latitude":      10,
	"payment_type":          11,
	"fare_amount":           12,
	"extra":                 13,
	"mta_tax":               14,
	"tip_amount":            15,
	"tolls_amount":          16,
	"total_amount":          18,
	"improvement_surcharge": 17,
}
