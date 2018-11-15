package fakeusers

import (
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/fake"
	"github.com/pkg/errors"
)

// Main holds the options for generating fake data and ingesting it to Pilosa.
type Main struct {
	Seed        int64    `help:"Random seed for generating data. -1 will use current nanosecond."`
	Num         uint64   `help:"Number of records to generate. 0 means infinity."`
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		Num:         0,
		PilosaHosts: []string{"localhost:10101"},
		Index:       "users",
		BatchSize:   1000,
	}
}

// Run begins generating data and ingesting it to Pilosa.
func (m *Main) Run() error {
	if m.Seed == -1 {
		m.Seed = time.Now().UnixNano()
	}

	src := fake.NewUserSource(m.Seed, m.Num)

	schema := gopilosa.NewSchema()
	idx := schema.Index("users")
	idx.Field("age", gopilosa.OptFieldTypeInt(0, 112))
	idx.Field("lastname", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 50000), gopilosa.OptFieldKeys(true))
	idx.Field("firstname", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 10000), gopilosa.OptFieldKeys(true))
	idx.Field("title", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 1000), gopilosa.OptFieldKeys(true))
	idx.Field("allergies", gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 1000), gopilosa.OptFieldKeys(true))

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, schema, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	for rec, err := src.Record(); err == nil; rec, err = src.Record() {
		pr := parseUserRecord(rec.(*fake.User))
		ingestPilosaRecord(indexer, pr)
	}
	return errors.Wrap(indexer.Close(), "closing indexer")
}

func parseUserRecord(u *fake.User) pdk.PilosaRecord {
	ret := pdk.PilosaRecord{
		Col: u.ID,
		Rows: []pdk.Row{
			{Field: "firstname", ID: u.FirstName},
			{Field: "lastname", ID: u.LastName},
			{Field: "title", ID: u.Title},
		},
		Vals: []pdk.Val{{Field: "age", Value: int64(u.Age)}},
	}
	for _, a := range u.Allergies {
		ret.Rows = append(ret.Rows, pdk.Row{Field: "allergies", ID: a})
	}
	return ret
}

func ingestPilosaRecord(indexer pdk.Indexer, pr pdk.PilosaRecord) {
	for _, row := range pr.Rows {
		indexer.AddColumn(row.Field, pr.Col, row.ID)
	}
	for _, val := range pr.Vals {
		indexer.AddValue(val.Field, pr.Col, val.Value)
	}

}
