package taxi

import "testing"

func TestFetchAndParse(t *testing.T) {
	/*
		looking over that again, it may not be clear what my intentions were
		`fetch` and `parse` are intended to be called as groups of goroutines
		so that you can control the concurrency with which urls are being fetched
		the the parser routines read from the record channel, parse the fields and write to pilosa
	*/

	/*
		fmt.Println("fetch and parse")
		url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv"

		urls := make(chan string)
		recs := make(chan string)

		fetch(urls, recs)
		parse(recs)

		urls <- url1
	*/
}

func TestTimeMapper(t *testing.T) {
	// TODO implement
}

func TestDayMapper(t *testing.T) {
	// TODO implement
}

func TestMonthMapper(t *testing.T) {
	// TODO implement
}

func TestIntMapper(t *testing.T) {
	// TODO implement
}

func TestSparseIntMapper(t *testing.T) {
	// TODO implement
}

func TestFloatMapper(t *testing.T) {
	// TODO implement
}

func TestArbitraryFloatMapper(t *testing.T) {
	// TODO implement
}

// func TestGridMapper(t *testing.T) {
// 	gm := pdk.GridMapper{
// 		Xmin: -5,
// 		Xmax: 5,
// 		Xres: 100,
// 		Ymin: -5,
// 		Ymax: 5,
// 		Yres: 100,
// 	}

// 	gmid, err := gm.ID(pdk.Point{X: -5, Y: -5})
// 	if err != nil || gmid != 0 {
// 		t.Fatalf("invalid results from gm.ID: %v, %v", gmid, err)
// 	}
// 	gmid, err = gm.ID(pdk.Point{X: -2.5, Y: 4.3})
// 	if err != nil || gmid != 2593 {
// 		t.Fatalf("invalid results from gm.ID: %v, %v", gmid, err)
// 	}
// 	gmid, err = gm.ID(pdk.Point{X: 0, Y: 8})
// 	if err == nil {
// 		t.Fatalf("out of bounds error not raised: %v", gmid)
// 	}

// }
