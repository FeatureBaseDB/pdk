package fake_test

import (
	"fmt"
	"testing"

	"github.com/pilosa/pdk/fake"
)

func TestRandomEvent(t *testing.T) {
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			event := fake.GenEvent()
			if event.ID == "" {
				t.Fatal("ID should not be empty")
			}
			if event.Station == "" {
				t.Fatal("Station should not be empty")
			}
			if event.UserID <= 0 {
				t.Fatal("UserID should be populated")
			}
			if event.Timestamp == "" {
				t.Fatal("Timestamp should not be empty")
			}
			if event.Velocity < 2500 || event.Velocity >= 3500 {
				t.Fatal("Velocity is out of range")
			}
			if event.Geo.TimeZone == "" {
				t.Fatal("TimeZone should not be empty")
			}
			if event.Geo.Latitude < -90.0 || event.Geo.Latitude > 90.0 {
				t.Fatalf("Latitude out of range: %v", event.Geo.Latitude)
			}
			if event.Geo.Longitude < 0.0 || event.Geo.Longitude > 360.0 {
				t.Fatalf("Longitude out of range: %v", event.Geo.Longitude)
			}
		})
	}
}
