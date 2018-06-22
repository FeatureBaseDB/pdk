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

package weather

import (
	"fmt"
	"net/http"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
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
	client   *gopilosa.Client
	index    *gopilosa.Index

	WeatherCache *weatherCache
}

// NewMain returns a new Main.
func NewMain() *Main {
	m := &Main{
		Concurrency:  1,
		WeatherCache: newWeatherCache(),
	}
	return m
}

func (m *Main) appendWeatherData() error {
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
	endTime := time.Date(2015, 12, 31, 0, 0, 0, 0, time.UTC)

	pickup_year, err := m.index.Field("pickup_year")
	if err != nil {
		return err
	}
	pickup_month, err := m.index.Field("pickup_month")
	if err != nil {
		return err
	}
	pickup_mday, err := m.index.Field("pickup_mday")
	if err != nil {
		return err
	}
	pickup_time, err := m.index.Field("pickup_time")
	if err != nil {
		return err
	}

	for t := startTime; endTime.After(t); t = t.Add(time.Hour) {
		timeBucket, _ := timeMapper.ID(t)
		q := m.index.Intersect(
			pickup_year.Row(uint64(t.Year())),
			pickup_month.Row(uint64(t.Month())),
			pickup_mday.Row(uint64(t.Day())),
			pickup_time.Row(uint64(timeBucket[0])),
		)
		response, _ := m.client.Query(q, nil)
		numBits := len(response.Result().Row().Columns)
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

		for _, ID := range response.Result().Row().Columns {
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
	return nil
}

// Run runs the weather usecase.
func (m *Main) Run() (err error) {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	schema := gopilosa.NewSchema()
	m.index, err = schema.Index(m.Index)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("describing index: %s", m.Index))
	}
	pdk.NewRankedField(m.index, "weather_condition", 0)
	pdk.NewRankedField(m.index, "precipitation_type", 0)
	pdk.NewRankedField(m.index, "precipitation_inches", 0)
	pdk.NewRankedField(m.index, "temp_f", 0)
	pdk.NewRankedField(m.index, "pressure_i", 0)
	pdk.NewRankedField(m.index, "humidity", 0)

	m.importer, err = pdk.SetupPilosa([]string{m.PilosaHost}, m.Index, schema, uint(m.BufferSize))
	if err != nil {
		return errors.Wrap(err, "setting up pilosa")
	}

	pilosaURI, err := gopilosa.NewURIFromAddress(m.PilosaHost)
	if err != nil {
		return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	}
	setupClient, err := gopilosa.NewClient(pilosaURI)
	if err != nil {
		return fmt.Errorf("setting up client: %v", err)
	}
	m.client = setupClient

	readFieldNames := []string{"cab_type", "passenger_count", "total_amount_dollars", "pickup_time", "pickup_day", "pickup_mday", "pickup_month", "pickup_year", "drop_time", "drop_day", "drop_mday", "drop_month", "drop_year", "dist_miles", "duration_minutes", "speed_mph", "pickup_grid_id", "drop_grid_id"}

	for _, fieldName := range readFieldNames {
		_, err := m.index.Field(fieldName)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("describing frame: %s", fieldName))
		}
	}
	err = setupClient.SyncSchema(schema)
	if err != nil {
		return errors.Wrap(err, "synchronizing schema")
	}

	err = m.WeatherCache.ReadAll()
	if err != nil {
		return errors.Wrap(err, "reading weather cache")
	}

	err = m.appendWeatherData()
	if err != nil {
		return errors.Wrap(err, "appending weather data")
	}

	return errors.Wrap(m.importer.Close(), "closing indexer")
}
