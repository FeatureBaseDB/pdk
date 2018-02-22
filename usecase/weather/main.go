package weather

import (
	"fmt"
	"net/http"
	"time"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main holds options and execution state for the weather usecase.
type Main struct {
	PilosaHost  string
	Concurrency int
	Index       string
	BufferSize  int
	URLFile     string

	importer pdk.Indexer
	client   *pcli.Client
	frames   map[string]*pcli.Frame
	index    *pcli.Index

	WeatherCache *WeatherCache
}

// NewMain returns a new Main.
func NewMain() *Main {
	m := &Main{
		Concurrency:  1,
		frames:       make(map[string]*pcli.Frame),
		WeatherCache: NewWeatherCache(),
	}
	return m
}

func (m *Main) appendWeatherData() {
	/*
		field     min      max        count
		temp:     1.900000 102.900000 161
		pressure: 28.530000 30.820000 215
		humidity: 11.000000 100.000000 89
		precip:   0.000000 1.840000 118
	*/

	timeMapper := pdk.TimeOfDayMapper{Res: 24}
	tempMapper := pdk.LinearFloatMapper{Min: -30, Max: 120, Res: 300}
	pressureMapper := pdk.LinearFloatMapper{Min: 28, Max: 31, Res: 300}
	humidityMapper := pdk.LinearFloatMapper{Min: 0, Max: 100, Res: 100}
	precipMapper := pdk.LinearFloatMapper{Min: 0, Max: 5, Res: 100}

	startTime := time.Date(2009, 2, 1, 0, 0, 0, 0, time.UTC)
	// endTime := time.Date(2009, 2, 28, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2015, 12, 31, 0, 0, 0, 0, time.UTC)

	for t := startTime; endTime.After(t); t = t.Add(time.Hour) {
		timeBucket, _ := timeMapper.ID(t)
		q := m.index.Intersect(
			m.frames["pickup_year"].Bitmap(uint64(t.Year())),
			m.frames["pickup_month"].Bitmap(uint64(t.Month())),
			m.frames["pickup_mday"].Bitmap(uint64(t.Day())),
			m.frames["pickup_time"].Bitmap(uint64(timeBucket[0])),
		)
		response, _ := m.client.Query(q, nil)
		numBits := len(response.Result().Bitmap().Bits)
		if numBits == 0 {
			continue
		}

		weather, err := m.WeatherCache.GetHourlyRecord(t)
		if err != nil {
			fmt.Printf("couldn't get weather data for %v: %v\n", t, err)
			continue
		}
		condID := uint64(*weather.Cond)
		precip, err1 := precipMapper.ID(float64(weather.Precipi))
		precipID := uint64(precip[0])
		temp, err2 := tempMapper.ID(float64(weather.Tempi))
		tempID := uint64(temp[0])
		pressure, err3 := pressureMapper.ID(float64(weather.Pressurei))
		pressureID := uint64(pressure[0])
		humid, err4 := humidityMapper.ID(float64(weather.Humidity))
		humidID := uint64(humid[0])

		for _, ID := range response.Result().Bitmap().Bits {
			// SetBit(weather.precip_code, ID, "precipitation_type")  // not implemented in weatherCache
			m.importer.AddBit("weather_condition", ID, condID)

			if err1 == nil && weather.Precipi > -100 {
				m.importer.AddBit("precipitation_inches", ID, precipID)
			}
			if err2 == nil {
				m.importer.AddBit("temp_f", ID, tempID)
			}
			if err3 == nil {
				m.importer.AddBit("pressure_i", ID, pressureID)
			}
			if err4 == nil && weather.Humidity > 10 {
				m.importer.AddBit("humidity", ID, humidID)
			}
		}
	}

}

// Run runs the weather usecase.
func (m *Main) Run() (err error) {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	readFrames := []string{"cab_type", "passenger_count", "total_amount_dollars", "pickup_time", "pickup_day", "pickup_mday", "pickup_month", "pickup_year", "drop_time", "drop_day", "drop_mday", "drop_month", "drop_year", "dist_miles", "duration_minutes", "speed_mph", "pickup_grid_id", "drop_grid_id"}
	writeFrames := []pdk.FrameSpec{
		pdk.NewRankedFrameSpec("weather_condition", 0),
		pdk.NewRankedFrameSpec("precipitation_type", 0),
		pdk.NewRankedFrameSpec("precipitation_inches", 0),
		pdk.NewRankedFrameSpec("temp_f", 0),
		pdk.NewRankedFrameSpec("pressure_i", 0),
		pdk.NewRankedFrameSpec("humidity", 0),
	}
	m.importer, err = pdk.SetupPilosa([]string{m.PilosaHost}, m.Index, writeFrames, uint(m.BufferSize))
	if err != nil {
		return errors.Wrap(err, "setting up pilosa")
	}

	pilosaURI, err := pcli.NewURIFromAddress(m.PilosaHost)
	if err != nil {
		return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	}
	setupClient := pcli.NewClientWithURI(pilosaURI)
	m.client = setupClient
	index, err := pcli.NewIndex(m.Index)
	m.index = index
	if err != nil {
		return fmt.Errorf("making index: %v", err)
	}
	err = setupClient.EnsureIndex(index)
	if err != nil {
		return fmt.Errorf("ensuring index existence: %v", err)
	}
	for _, frame := range readFrames {
		fram, err := index.Frame(frame, &pcli.FrameOptions{})
		m.frames[frame] = fram
		if err != nil {
			return fmt.Errorf("making frame: %v", err)
		}
		err = setupClient.EnsureFrame(fram)
		if err != nil {
			return fmt.Errorf("creating frame '%v': %v", frame, err)
		}
	}

	err = m.WeatherCache.ReadAll()
	if err != nil {
		return errors.Wrap(err, "reading weather cache")
	}

	m.appendWeatherData()

	return errors.Wrap(m.importer.Close(), "closing indexer")
}
