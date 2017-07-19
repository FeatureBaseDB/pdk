package weather

import (
	"fmt"
	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"net/http"
	"time"
)

func (m *Main) testCache() {
	fmt.Printf("Read %d weather records\n", len(m.weatherCache.data))
	day, _ := m.weatherCache.GetDailyRecord(time.Date(2010, 4, 3, 0, 0, 0, 0, time.UTC))
	fmt.Printf("%+v\n", day)
	hour, _ := m.weatherCache.GetHourlyRecord(time.Date(2010, 4, 3, 3, 0, 0, 0, time.UTC))
	fmt.Printf("%+v\n", hour)

	day, err := m.weatherCache.GetDailyRecord(time.Date(2003, 4, 3, 0, 0, 0, 0, time.UTC))
	fmt.Printf("%+v %v\n", day, err)
	hour, err = m.weatherCache.GetHourlyRecord(time.Date(2003, 4, 3, 3, 0, 0, 0, time.UTC))
	fmt.Printf("%+v %v\n", hour, err)
}

type Main struct {
	PilosaHost  string
	Concurrency int
	Index       string
	BufferSize  int

	importer pdk.PilosaImporter
	client   *pcli.Client
	frames   map[string]*pcli.Frame
	index    *pcli.Index

	weatherCache *WeatherCache
}

func NewMain() *Main {
	m := &Main{
		Concurrency:  1,
		frames:       make(map[string]*pcli.Frame),
		weatherCache: NewWeatherCache("usecase/weather/urls.txt"),
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
		weather, err := m.weatherCache.GetHourlyRecord(t)
		if err != nil {
			fmt.Printf("couldn't get weather data for %v\n", t)
			continue
		}
		timeBucket, _ := timeMapper.ID(t)
		q := m.index.Intersect(
			m.frames["pickup_year"].Bitmap(uint64(t.Year())),
			m.frames["pickup_month"].Bitmap(uint64(t.Month())),
			m.frames["pickup_mday"].Bitmap(uint64(t.Day())),
			m.frames["pickup_time"].Bitmap(uint64(timeBucket[0])),
		)
		response, _ := m.client.Query(q, nil)

		for _, ID := range response.Result().Bitmap.Bits {
			// SetBit(weather.precip_code, ID, "precipitation_type")  // not implemented in weatherCache
			m.importer.SetBit(uint64(*weather.Cond), ID, "weather_condition")
			precip, err := precipMapper.ID(float64(weather.Precipi))
			if err != nil {
				m.importer.SetBit(uint64(precip[0]), ID, "precipitation_inches")
			}
			temp, err := tempMapper.ID(float64(weather.Tempi))
			if err != nil {
				m.importer.SetBit(uint64(temp[0]), ID, "temp_f")
			}
			pressure, err := pressureMapper.ID(float64(weather.Pressurei))
			if err != nil {
				m.importer.SetBit(uint64(pressure[0]), ID, "pressure_i")
			}
			humid, err := humidityMapper.ID(float64(weather.Humidity))
			if err != nil {
				m.importer.SetBit(uint64(humid[0]), ID, "humidity")
			}
		}
	}

}

func (m *Main) Run() error {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	readFrames := []string{"cab_type", "passenger_count", "total_amount_dollars", "pickup_time", "pickup_day", "pickup_mday", "pickup_month", "pickup_year", "drop_time", "drop_day", "drop_mday", "drop_month", "drop_year", "dist_miles", "duration_minutes", "speed_mph", "pickup_grid_id", "drop_grid_id"}
	writeFrames := []string{"weather_condition", "precipitation_type", "precipitation_inches", "temp_f", "pressure_i", "humidity"}
	m.importer = pdk.NewImportClient(m.PilosaHost, m.Index, writeFrames, m.BufferSize)

	pilosaURI, err := pcli.NewURIFromAddress(m.PilosaHost)
	if err != nil {
		return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	}
	setupClient := pcli.NewClientWithURI(pilosaURI)
	m.client = setupClient
	index, err := pcli.NewIndex(m.Index, &pcli.IndexOptions{})
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
	for _, frame := range writeFrames {
		fram, err := index.Frame(frame, &pcli.FrameOptions{})
		if err != nil {
			return fmt.Errorf("making frame: %v", err)
		}
		err = setupClient.EnsureFrame(fram)
		if err != nil {
			return fmt.Errorf("creating frame '%v': %v", frame, err)
		}
	}

	m.appendWeatherData()

	m.importer.Close()
	return nil
}
