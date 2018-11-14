package fakeusers

import (
	"log"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/fake"
	"github.com/pkg/errors"
)

// Main holds the options for generating fake data and ingesting it to Pilosa.
type Main struct {
	Seed        int64  `help:"Random seed for generating data. -1 will use current nanosecond."`
	Num         uint64 `help:"Number of records to generate. 0 means infinity."`
	Framer      pdk.DashField
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	Proxy       string   `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		Num:         0,
		PilosaHosts: []string{"localhost:10101"},
		Index:       "users",
		BatchSize:   1000,
		Proxy:       ":13131",
	}
}

// Run begins generating data and ingesting it to Pilosa.
func (m *Main) Run() error {
	if m.Seed == -1 {
		m.Seed = time.Now().UnixNano()
	}

	src := fake.NewUserSource(m.Seed, m.Num)

	parser := pdk.NewDefaultGenericParser()

	mapper := pdk.NewCollapsingMapper()
	mapper.ColTranslator = nil
	mapper.Framer = &m.Framer

	schema := gopilosa.NewSchema()
	idx := schema.Index("users")
	idx.Field("age", gopilosa.OptFieldTypeInt(0, 112))
	idx.Field("lastname", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 50000))
	idx.Field("firstname", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 10000))
	idx.Field("title", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 1000))
	idx.Field("allergies", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 1000))

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, schema, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	go func() {
		err = pdk.StartMappingProxy(m.Proxy, pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator))
		log.Fatal(errors.Wrap(err, "starting mapping proxy"))
	}()
	return errors.Wrap(ingester.Run(), "running ingester")
}
