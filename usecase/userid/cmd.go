package userid

import (
	"math/rand"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// Main holds the options for generating fake data and ingesting it to Pilosa.
type Main struct {
	Seed        int64    `help:"Random seed for generating data. -1 will use current nanosecond."`
	Num         uint64   `help:"Number of records to generate. 0 means infinity."`
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	Field       string   `help:"Field to put user IDs into."`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		Num:         1266087512, // size of full taxi dataset
		PilosaHosts: []string{"localhost:10101"},
		Index:       "taxi",
		BatchSize:   100000,
		Field:       "user_id",
	}
}

// Run begins generating data and ingesting it to Pilosa.
func (m *Main) Run() error {
	if m.Seed == -1 {
		m.Seed = time.Now().UnixNano()
	}

	schema := gopilosa.NewSchema()
	idx := schema.Index(m.Index)
	idx.Field(m.Field, gopilosa.OptFieldTypeSet(gopilosa.CacheTypeRanked, 50000))
	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, schema, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ug := newUserGetter(m.Seed)

	for i := uint64(0); i < m.Num; i++ {
		indexer.AddColumn(m.Field, i, ug.ID())
	}

	return errors.Wrap(indexer.Close(), "closing indexer")
}

type userGetter struct {
	z *rand.Zipf
}

func newUserGetter(seed int64) *userGetter {
	r := rand.New(rand.NewSource(seed))
	return &userGetter{
		z: rand.NewZipf(r, 1.1, 1024, 5000000), // zipfian distribution over 5 million users
	}
}

func (u *userGetter) ID() uint64 {
	return u.z.Uint64()
}
