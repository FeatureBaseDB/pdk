package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pdk"
	"github.com/umbel/pilosa"
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

var urls = []string{
	"https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv",
	"https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-09.csv",
	"https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-10.csv",
	"https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-11.csv",
	"https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-12.csv",
}

var db = "taxi"

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

func main() {
	fmt.Println("fetch and parse")
	url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv"
	//url2 := "http://alanbernstein.net/files/green_tripdata_sample.csv"

	//urls := make(chan string)
	//recs := make(chan string)
	//go fetch(urls, recs)
	//go parse(recs)
	//urls <- url2

	pms := getParserMappers()

	nexter := Nexter{}

	err := fetch(url1, &nexter, pms)
	if err != nil {
		fmt.Println(err)
	}
}

func getParserMappers() []pdk.ParserMapper {
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
	fm := pdk.FloatMapper{
		Buckets: []float64{0, 0.5, 1, 2, 5, 10, 25, 50, 100, 200},
	}

	// map the (pickupTime, dropTime) pair, according to the duration in minutes, binned using `fm`
	durm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			return end.Sub(start).Minutes()
		},
		Mapper: fm,
	}

	// map the (pickupTime, dropTime, dist) triple, according to the speed in mph, binned using `fm`
	speedm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			dist := fields[2].(float64)
			return dist / end.Sub(start).Hours()
		},
		Mapper: fm,
	}

	tp := pdk.TimeParser{Layout: layout}

	pms := []pdk.ParserMapper{

		pdk.ParserMapper{
			Frame:   "passengerCount",
			Mapper:  pdk.IntMapper{Min: 1, Max: 8},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{Passenger_count},
		},
		pdk.ParserMapper{
			Frame:   "totalAmount",
			Mapper:  pdk.LinearFloatMapper{Min: 0, Max: 200, Res: 100},
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{Total_amount},
		},
		pdk.ParserMapper{
			Frame:   "cabType",
			Mapper:  pdk.IntMapper{Min: 0, Max: 2},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{VendorID},
		},
		pdk.ParserMapper{
			Frame:   "pickupTime",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.ParserMapper{
			Frame:   "pickupDay",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.ParserMapper{
			Frame:   "pickupMonth",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_pickup_datetime},
		},
		pdk.ParserMapper{
			Frame:   "dropTime",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.ParserMapper{
			Frame:   "dropDay",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.ParserMapper{
			Frame:   "dropMonth",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{Lpep_dropoff_datetime},
		},
		pdk.ParserMapper{
			Frame:   "dist_miles", // note "_miles" is a unit annotation
			Mapper:  fm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{Trip_distance},
		},
		pdk.ParserMapper{
			Frame:   "duration_minutes",
			Mapper:  durm,
			Parsers: []pdk.Parser{tp, tp},
			Fields:  []int{Lpep_pickup_datetime, Lpep_dropoff_datetime},
		},
		pdk.ParserMapper{
			Frame:   "speed_mph",
			Mapper:  speedm,
			Parsers: []pdk.Parser{tp, tp, pdk.FloatParser{}},
			Fields:  []int{Lpep_pickup_datetime, Lpep_dropoff_datetime, Trip_distance},
		},
		pdk.ParserMapper{
			Frame:   "pickupGridID",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{Pickup_longitude, Pickup_latitude},
		},
		pdk.ParserMapper{
			Frame:   "dropGridID",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{Dropoff_longitude, Dropoff_latitude},
		},
	}

	return pms
}

func fetch(url string, nexter *Nexter, parserMappers []pdk.ParserMapper) error {
	fmt.Println(url)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
		return err
	}

	scan := bufio.NewScanner(resp.Body)
	for scan.Scan() {
		s := scan.Text()
		// parse(s)
		parseNew(s, nexter, parserMappers)
	}

	return nil
}

func fetch_async(urls <-chan string, recs chan<- string) error {
	fmt.Println("fetch")
	for url := range urls {
		fmt.Println(url)
		resp, err := http.Get(url)
		fmt.Println(resp)
		fmt.Println(err)
		if err != nil {
			fmt.Println(err)
			return err
		}

		scan := bufio.NewScanner(resp.Body)
		fmt.Println("scan loop")
		for scan.Scan() {
			s := scan.Text()
			fmt.Println(s)
			recs <- s
		}
	}
	return nil
}

func parse(rec string) {
	fields := strings.Split(rec, ",")
	if len(fields) <= 1 {
		log.Printf("Empty line")
		return
	}
	pickupTime, err := time.Parse(layout, fields[Lpep_pickup_datetime])
	if err != nil {
		log.Printf("Couldn't parse time: %v, err: %v", fields[Lpep_pickup_datetime], err)
		return
	}
	dropTime, err := time.Parse(layout, fields[Lpep_dropoff_datetime])
	if err != nil {
		log.Printf("Couldn't parse time: %v, err: %v", fields[Lpep_dropoff_datetime], err)
		return
	}

	dist, err := strconv.ParseFloat(fields[Trip_distance], 32)
	if err != nil || dist == 0 {
		if dist == 0 {
			log.Printf("Invalid distance %v: %v", fields[Trip_distance], err)
		}
		return
	}
	vendorID, err := strconv.Atoi(fields[VendorID])
	pickLoc, dropLoc, err := getLocations(fields)

	post(vendorID, pickupTime, dropTime, dist, pickLoc, dropLoc)
}

func parseNew(rec string, nexter *Nexter, parserMappers []pdk.ParserMapper) {
	// TODO: consider optimizing this
	client, err := pilosa.NewClient("localhost:15000")
	if err != nil {
		panic(err)
	}
	qb := &QueryBuilder{}
	qb.profileID = nexter.Next()
	qb.query = ""

	fields := strings.Split(rec, ",")

	for _, pm := range parserMappers {
		if len(pm.Fields) != len(pm.Parsers) {
			// TODO if len(pm.Parsers) == 1, use that for all fields
			fmt.Println("parse: ParserMapper has different number of fields and parsers")
			return
		}
		// parse fields into a slice `parsed`
		parsed := make([]interface{}, 0, len(pm.Fields))
		for n, fieldnum := range pm.Fields {
			parser := pm.Parsers[n]
			if fieldnum >= len(fields) {
				// TODO this check doesn't need to be in the inner loop
				fmt.Println("parse: field index out of range")
				return
			}
			parsedField, err := parser.Parse(fields[fieldnum])
			if err != nil {
				fmt.Println(err)
				return
			}
			parsed = append(parsed, parsedField)
		}

		// map those fields to a slice of IDs
		ids, err := pm.Mapper.ID(parsed...)
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, id := range ids {
			// TODO bitmap attributes
			qb.Add(uint64(id), pm.Frame)
		}
	}
	fmt.Println(qb.Query())
	res, err := client.ExecuteQuery(context.Background(), db, qb.Query(), true)
	log.Println("result: ", res, err)
}

func parseAsync(recs <-chan string) {
	fmt.Println("parse")
	for rec := range recs {
		fmt.Println(rec)
		fields := strings.Split(rec, ",")
		pickupTime, err := time.Parse(layout, fields[Lpep_pickup_datetime])
		if err != nil {
			log.Printf("Couldn't parse time: %v, err: %v", fields[Lpep_pickup_datetime], err)
			continue
		}
		dropTime, err := time.Parse(layout, fields[Lpep_dropoff_datetime])
		if err != nil {
			log.Printf("Couldn't parse time: %v, err: %v", fields[Lpep_dropoff_datetime], err)
			continue
		}
		dist, err := strconv.ParseFloat(fields[Trip_distance], 32)
		if err != nil || dist == 0 {
			if dist == 0 {
				log.Printf("Couldn't convert %v to float: %v", fields[Trip_distance], err)
			}
			continue
		}
		vendorID, err := strconv.Atoi(fields[VendorID])
		pickLoc, dropLoc, err := getLocations(fields)

		post(vendorID, pickupTime, dropTime, dist, pickLoc, dropLoc)
	}
}

func getLocations(fields []string) (pickup pdk.Point, dropoff pdk.Point, err error) {
	// parse locations
	pickup, dropoff = pdk.Point{}, pdk.Point{}
	pickup.X, err = strconv.ParseFloat(fields[Pickup_longitude], 64)
	if err != nil {
		return pickup, dropoff, err
	}
	pickup.Y, err = strconv.ParseFloat(fields[Pickup_latitude], 64)
	if err != nil {
		return pickup, dropoff, err
	}
	dropoff.X, err = strconv.ParseFloat(fields[Dropoff_longitude], 64)
	if err != nil {
		return pickup, dropoff, err
	}
	dropoff.Y, err = strconv.ParseFloat(fields[Dropoff_latitude], 64)
	return pickup, dropoff, err
}

func post(cabType int, pickupTime, dropTime time.Time, dist float64, pickupLoc, dropLoc pdk.Point) {
	fmt.Println(cabType, pickupTime, dropTime, dist, pickupLoc, dropLoc)

	im := pdk.IntMapper{
		Min: 0,
		Max: 2,
	}

	// map a timestamp according to its cyclic components
	tm := pdk.TimeOfDayMapper{Res: 48}
	dm := pdk.DayOfWeekMapper{}
	mm := pdk.MonthMapper{}

	// map a point (pair of floats) to a grid sector of a rectangular region
	gm := pdk.GridMapper{
		Xmin: -74.27,
		Xmax: -73.69,
		Xres: 100,
		Ymin: 40.48,
		Ymax: 40.93,
		Yres: 100,
	}

	// map a float according to a custom set of bins
	fm := pdk.FloatMapper{
		Buckets: []float64{0, 0.5, 1, 2, 5, 10, 25, 50, 100, 200},
	}

	// map the (pickupTime, dropTime) pair, according to the duration in minutes, binned using `fm`
	durm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			return end.Sub(start).Minutes()
		},
		Mapper: fm,
	}

	// map the (pickupTime, dropTime, dist) triple, according to the speed in mph, binned using `fm`
	speedm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			dist := fields[2].(float64)
			return dist / end.Sub(start).Hours()
		},
		Mapper: fm,
	}

	cabTypeID, err := im.ID(int64(cabType))
	if err != nil {
		fmt.Println(err)
		return
	}
	pickupTimeID, err := tm.ID(pickupTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	dropTimeID, err := tm.ID(dropTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	pickupDayID, err := dm.ID(pickupTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	dropDayID, err := dm.ID(dropTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	pickupMonthID, err := mm.ID(pickupTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	dropMonthID, err := mm.ID(dropTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	distCustomID, err := fm.ID(dist)
	if err != nil {
		fmt.Println(err)
		return
	}
	durID, err := durm.ID(pickupTime, dropTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	speedID, err := speedm.ID(pickupTime, dropTime, dist)
	if err != nil {
		fmt.Println(err)
		return
	}
	pickupLocID, err := gm.ID(pickupLoc)
	if err != nil {
		fmt.Println(err)
		return
	}
	dropLocID, err := gm.ID(dropLoc)
	if err != nil {
		fmt.Println(err)
		return
	}

	// SetBit
	rideID := -1
	fmt.Printf("SetBit(id=%d, frame='cabType', profileID=%d)\n", cabTypeID, rideID)
	fmt.Printf("SetBit(id=%d, frame='pickupTime', profileID=%d)\n", pickupTimeID, rideID)
	fmt.Printf("SetBit(id=%d, frame='pickupDay', profileID=%d)\n", pickupDayID, rideID)
	fmt.Printf("SetBit(id=%d, frame='pickupMonth', profileID=%d)\n", pickupMonthID, rideID)
	fmt.Printf("SetBit(id=%d, frame='dropTime', profileID=%d)\n", dropTimeID, rideID)
	fmt.Printf("SetBit(id=%d, frame='dropDay', profileID=%d)\n", dropDayID, rideID)
	fmt.Printf("SetBit(id=%d, frame='dropMonth', profileID=%d)\n", dropMonthID, rideID)
	fmt.Printf("SetBit(id=%d, frame='dist_miles', profileID=%d)\n", distCustomID, rideID)
	fmt.Printf("SetBit(id=%d, frame='duration_minutes', profileID=%d)\n", durID, rideID)
	fmt.Printf("SetBit(id=%d, frame='speed_mph', profileID=%d)\n", speedID, rideID)
	fmt.Printf("SetBit(id=%d, frame='pickupGridID', profileID=%d)\n", pickupLocID, rideID)
	fmt.Printf("SetBit(id=%d, frame='dropGridID', profileID=%d)\n", dropLocID, rideID)
}
