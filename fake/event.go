package fake

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pilosa/pdk/fake/gen"
)

var globalGen = NewEventGenerator(0)

// Event is an example event that includes a variety of types.
type Event struct {
	// ID is a unique event identifier
	ID string `json:"id"`

	// Station is a medium cardinality (1000s) string that has a 1 to many association with Events
	Station string `json:"station"`

	// UserID is a high cardinality (100s of millions) identifier that has a 1 to many association with events.
	UserID int `json:"user_id"`

	// Time the event occured. TODO how is it encoded?
	Timestamp string `json:"timestamp"`

	// A set of medium cardinality items to associate with this event.
	Favorites []string `json:"favorites"`

	// Set of complex objects.
	Items []Item `json:"items"`

	// Ordered list of objects.
	Ranking []Item `json:"ranking"`

	IfaceThing Interface `json:"ifacething"`

	// An integer with a set range of possible values to associate with this event.
	Velocity int `json:"velocity"`

	// A boolean to associate with this event.
	Active bool `json:"active"`

	// The location of this event.
	Geo Geo `json:"geo"`
}

// Interface exists to make sure that parsing code can handle interface
// values.
type Interface interface {
	isFake()
}

// String wraps string so that it can implement Interface
type String string

func (String) isFake() {}

// Geo represents a location.
type Geo struct {
	// Low cardinality association.
	TimeZone string `json:"timezone"`

	// Fine grained location.
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
}

// Item has a name and integer value.
type Item struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// EventGenerator generates random events.
type EventGenerator struct {
	r *rand.Rand
	g *gen.Generator
}

// NewEventGenerator gets a new EventGenerator.
func NewEventGenerator(seed int64) *EventGenerator {
	return &EventGenerator{
		r: rand.New(rand.NewSource(seed)),
		g: gen.NewGenerator(seed + 1),
	}
}

// Event generates a random event.
func (g *EventGenerator) Event() *Event {
	active := false
	if g.r.Intn(2) > 0 {
		active = true
	}
	return &Event{
		ID:         fmt.Sprintf("%d", g.r.Uint64()),
		Station:    g.g.String(6, 2000),
		UserID:     int(g.g.Uint64(100000000)) + 1,
		Timestamp:  g.g.Time(time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC), time.Second*3).Format(time.RFC3339),
		Favorites:  g.genFavorites(),
		Items:      g.genItems(),
		Ranking:    g.genItems(),
		IfaceThing: String(g.g.String(5, 5)),
		Velocity:   g.r.Intn(1000) + 2500,
		Active:     active,
		Geo:        g.genGeo(),
	}

}

// GenEvent builds a random Event.
func GenEvent() *Event {
	return globalGen.Event()
}

func (g *EventGenerator) genFavorites() []string {
	n := g.r.Intn(7)
	favs := make([]string, n)
	for i := 0; i < n; i++ {
		done := false
		for !done {
			done = true
			favs[i] = g.g.String(8, 1000)
			for j := 0; j < i; j++ {
				if favs[i] == favs[j] {
					done = false
					break
				}
			}
		}
	}
	return favs
}

var timezones = []string{
	"ACDT", "ACST", "ACT", "ACT", "ACWST", "ADT", "AEDT", "AEST", "AFT", "AKDT", "AKST", "AMST",
	"AMT", "AMT", "ART", "AST", "AST", "AWST", "AZOST", "AZOT", "AZT", "BDT", "BIOT", "BIT",
	"BOT", "BRST", "BRT", "BST", "BST", "BST", "BTT", "CAT", "CCT", "CDT", "CDT", "CEST",
	"CET", "CHADT", "CHAST", "CHOT", "CHOST", "CHST", "CHUT", "CIST", "CIT", "CKT", "CLST", "CLT",
	"COST", "COT", "CST", "CST", "CST", "CT", "CVT", "CWST", "CXT", "DAVT", "DDUT", "DFT",
	"EASST", "EAST", "EAT", "ECT", "ECT", "EDT", "EEST", "EET", "EGST", "EGT", "EIT", "EST",
	"FET", "FJT", "FKST", "FKT", "FNT", "GALT", "GAMT", "GET", "GFT", "GILT", "GIT", "GMT",
	"GST", "GST", "GYT", "HDT", "HAEC", "HST", "HKT", "HMT", "HOVST", "HOVT", "ICT", "IDT",
	"IOT", "IRDT", "IRKT", "IRST", "IST", "IST", "IST", "JST", "KGT", "KOST", "KRAT", "KST",
	"LHST", "LHST", "LINT", "MAGT", "MART", "MAWT", "MDT", "MET", "MEST", "MHT", "MIST", "MIT",
	"MMT", "MSK", "MST", "MST", "MUT", "MVT", "MYT", "NCT", "NDT", "NFT", "NPT", "NST",
	"NT", "NUT", "NZDT", "NZST", "OMST", "ORAT", "PDT", "PET", "PETT", "PGT", "PHOT", "PHT",
	"PKT", "PMDT", "PMST", "PONT", "PST", "PST", "PYST", "PYT", "RET", "ROTT", "SAKT", "SAMT",
	"SAST", "SBT", "SCT", "SDT", "SGT", "SLST", "SRET", "SRT", "SST", "SST", "SYOT", "TAHT",
	"THA", "TFT", "TJT", "TKT", "TLT", "TMT", "TRT", "TOT", "TVT", "ULAST", "ULAT", "USZ1",
	"UTC", "UYST", "UYT", "UZT", "VET", "VLAT", "VOLT", "VOST", "VUT", "WAKT", "WAST", "WAT",
	"WEST", "WET", "WIT", "WST", "YAKT", "YEKT",
}

func (g *EventGenerator) genGeo() Geo {
	return Geo{
		TimeZone:  timezones[gen.Uint64(len(timezones))],
		Longitude: float64(g.g.Uint64(360000)) / 1000.0,
		Latitude:  (float64(g.g.Uint64(180000)) / 1000.0) - 90.0,
	}
}

func (g *EventGenerator) genItems() []Item {
	n := g.r.Intn(5)
	items := make([]Item, n)
	for i := 0; i < n; i++ {
		items[i] = g.genItem()
	}
	return items
}

func (g *EventGenerator) genItem() Item {
	return Item{
		Name:  g.g.String(8, 1000),
		Value: int(g.r.Int31()),
	}
}
