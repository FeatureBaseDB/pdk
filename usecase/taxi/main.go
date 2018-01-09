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

	_ "net/http/pprof"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
)

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

/***********************
use case implementation
***********************/

// TODO autoscan 1. determine field type by attempting conversions
// TODO autoscan 2. determine field mapping by looking at statistics (for floatmapper, intmapper)
// TODO autoscan 3. write results from ^^ to config file
// TODO read ParserMapper config from file (cant do CustomMapper)

type Main struct {
	PilosaHost       string
	URLFile          string
	FetchConcurrency int
	Concurrency      int
	Index            string
	BufferSize       int

	importer  pdk.PilosaImporter
	urls      []string
	greenBms  []pdk.BitMapper
	yellowBms []pdk.BitMapper
	ams       []pdk.AttrMapper

	nexter *Nexter

	totalBytes int64
	bytesLock  sync.Mutex

	totalRecs     *Counter
	skippedRecs   *Counter
	nullLocs      *Counter
	badLocs       *Counter
	badSpeeds     *Counter
	badTotalAmnts *Counter
	badDurations  *Counter
	badPassCounts *Counter
	badDist       *Counter
	badUnknowns   *Counter
}

func NewMain() *Main {
	m := &Main{
		Concurrency:      1,
		FetchConcurrency: 1,
		nexter:           &Nexter{},
		urls:             make([]string, 0),

		totalRecs:     &Counter{},
		skippedRecs:   &Counter{},
		nullLocs:      &Counter{},
		badLocs:       &Counter{},
		badSpeeds:     &Counter{},
		badTotalAmnts: &Counter{},
		badDurations:  &Counter{},
		badPassCounts: &Counter{},
		badDist:       &Counter{},
		badUnknowns:   &Counter{},
	}

	return m
}

func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := m.readURLs()
	if err != nil {
		return err
	}

	frames := []string{"cab_type", "passenger_count", "total_amount_dollars", "pickup_time", "pickup_day", "pickup_mday", "pickup_month", "pickup_year", "drop_time", "drop_day", "drop_mday", "drop_month", "drop_year", "dist_miles", "duration_minutes", "speed_mph", "pickup_grid_id", "drop_grid_id", "pickup_elevation", "drop_elevation"}
	m.importer = pdk.NewImportClient(m.PilosaHost, m.Index, frames, m.BufferSize)

	pilosaURI, err := pcli.NewURIFromAddress(m.PilosaHost)
	if err != nil {
		return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	}
	setupClient := pcli.NewClientWithURI(pilosaURI)
	index, err := pcli.NewIndex(m.Index)
	if err != nil {
		return fmt.Errorf("making index: %v", err)
	}
	err = setupClient.EnsureIndex(index)
	if err != nil {
		return fmt.Errorf("ensuring index existence: %v", err)
	}
	for _, frame := range frames {
		fram, err := index.Frame(frame, pcli.CacheTypeRanked)
		if err != nil {
			return fmt.Errorf("making frame: %v", err)
		}
		err = setupClient.EnsureFrame(fram)
		if err != nil {
			return fmt.Errorf("creating frame '%v': %v", frame, err)
		}
	}

	ticker := m.printStats()

	urls := make(chan string, 100)
	records := make(chan Record, 10000)

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
			log.Printf("Rides: %d, Bytes: %s", m.nexter.Last(), pdk.Bytes(m.BytesProcessed()))
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
	m.importer.Close()
	ticker.Stop()
	return err
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
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
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
		for url, _ := range failedURLs {
			return url, true
		}
		return "", false
	}
	return url, true
}

func (m *Main) fetch(urls <-chan string, records chan<- Record) {
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

		scan := bufio.NewScanner(buf)
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
			record := scan.Text()
			m.AddBytes(len(record))
			if correctLine {
				// last field needs to be shifted over by 1
				lastcomma := strings.LastIndex(record, ",")
				if lastcomma == -1 {
					m.skippedRecs.Add(1)
					continue
				}
				record = record[:lastcomma] + "," + record[lastcomma:]
			}
			records <- Record{Val: record, Type: typ}
		}
		err = scan.Err()
		if err != nil {
			log.Printf("scan error on %s, err: %v", url, err)
		}
		delete(failedURLs, url)
	}
}

type Record struct {
	Type rune
	Val  string
}

func (r Record) Clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

type BitFrame struct {
	Bit   uint64
	Frame string
}

func (m *Main) parseMapAndPost(records <-chan Record) {
Records:
	for record := range records {
		fields, ok := record.Clean()
		if !ok {
			m.skippedRecs.Add(1)
			continue
		}
		var bms []pdk.BitMapper
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
		bitsToSet := make([]BitFrame, 0)
		bitsToSet = append(bitsToSet, BitFrame{Bit: cabType, Frame: "cab_type"})
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
				if strings.Contains(bm.Frame, "grid_id") && strings.Contains(err.Error(), "out of range") {
					m.badLocs.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Frame == "speed_mph" && strings.Contains(err.Error(), "out of range") {
					m.badSpeeds.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Frame == "total_amount_dollars" && strings.Contains(err.Error(), "out of range") {
					m.badTotalAmnts.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Frame == "duration_minutes" && strings.Contains(err.Error(), "out of range") {
					m.badDurations.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Frame == "passenger_count" && strings.Contains(err.Error(), "out of range") {
					m.badPassCounts.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Frame == "dist_miles" && strings.Contains(err.Error(), "out of range") {
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
				bitsToSet = append(bitsToSet, BitFrame{Bit: uint64(id), Frame: bm.Frame})
			}
		}
		columnID := m.nexter.Next()
		for _, bit := range bitsToSet {
			m.importer.SetBit(bit.Bit, columnID, bit.Frame)
		}
	}
}

func getAttrMappers() []pdk.AttrMapper {
	ams := []pdk.AttrMapper{}

	return ams
}

func getBitMappers(fields map[string]int) []pdk.BitMapper {
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

	bms := []pdk.BitMapper{
		pdk.BitMapper{
			Frame:   "passenger_count",
			Mapper:  pdk.IntMapper{Min: 0, Max: 9},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{fields["passenger_count"]},
		},
		pdk.BitMapper{
			Frame:   "total_amount_dollars",
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{fields["total_amount"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_time",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_mday",
			Mapper:  pdk.DayOfMonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_month",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_year",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_time",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_mday",
			Mapper:  pdk.DayOfMonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_month",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_year",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "dist_miles", // note "_miles" is a unit annotation
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{fields["trip_distance"]},
		},
		pdk.BitMapper{
			Frame:   "duration_minutes",
			Mapper:  durm,
			Parsers: []pdk.Parser{tp, tp},
			Fields:  []int{fields["pickup_datetime"], fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "speed_mph",
			Mapper:  speedm,
			Parsers: []pdk.Parser{tp, tp, pdk.FloatParser{}},
			Fields:  []int{fields["pickup_datetime"], fields["dropoff_datetime"], fields["trip_distance"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_grid_id",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["pickup_longitude"], fields["pickup_latitude"]},
		},
		pdk.BitMapper{
			Frame:   "drop_grid_id",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_elevation",
			Mapper:  gfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
		pdk.BitMapper{
			Frame:   "drop_elevation",
			Mapper:  gfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
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

type Counter struct {
	num  int64
	lock sync.Mutex
}

func (c *Counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}

// Nexter generates unique sequential ids in a threadsafe way.
type Nexter struct {
	id   uint64
	lock sync.Mutex
}

// Next generates a new id
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
