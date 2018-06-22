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
