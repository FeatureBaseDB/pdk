package genome

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"net/http"
	_ "net/http/pprof"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

const SLICEWIDTH = 8388608
const BASECOUNT = 4

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Chromosome holds FASTA data in a string for a particular chromosome.
type Chromosome struct {
	data   string
	number int
	offset uint64
}

type Gene struct {
	chromosome uint64
	start      uint64
	end        uint64
	name       string
}

var genes = []Gene{
	{chromosome: 1, start: 156052369, end: 156109880, name: "LMNA"},
	{chromosome: 23, start: 31097677, end: 33339441, name: "DMD"},
	{chromosome: 7, start: 146116002, end: 148420998, name: "CNTNAP2"},
	{chromosome: 9, start: 8314246, end: 10612723, name: "PTPRD"},
	{chromosome: 2, start: 178525989, end: 178807423, name: "TTN"},
	{chromosome: 2, start: 151485334, end: 151734487, name: "NEB"},
	{chromosome: 6, start: 152121684, end: 152637399, name: "SYNE1"},
}

// Main holds the config for the http command.
type Main struct {
	File              string   `help:"Path to FASTA file."`
	Hosts             []string `help:"Pilosa hosts."`
	Index             string   `help:"Pilosa index."`
	Min               float64  `help:"Minimum fraction of random mutations."`
	Max               float64  `help:"Maximum fraction of random mutations."`
	Count             uint64   `help:"Number of mutated rows to create."`
	Concurrency       int      `help:"Number of slice importers to run simultaneously."`
	ImportGenes       bool     `help:"Import gene masks."`
	ImportChromosomes bool     `help:"Import chromosome masks."`
	ImportSequences   bool     `help:"Import reference and mutated sequences."`

	index pdk.Indexer

	chromosomes []*Chromosome
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Hosts:       []string{":10101"},
		Index:       "genome",
		Min:         0.001,
		Max:         0.005,
		Count:       10,
		Concurrency: 8,
	}
}

var frame = "sequences"

var frames = []pdk.FrameSpec{
	pdk.NewRankedFrameSpec(frame, 100000),
	pdk.NewRankedFrameSpec("chromosomes", 100000),
	pdk.NewRankedFrameSpec("genes", 100000),
}

// Run runs the genome command.
func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var err error

	// Load FASTA file.
	start := time.Now()
	log.Printf("Start file load at %v", start)
	err = m.loadFile(m.File)
	if err != nil {
		return errors.Wrap(err, "loading file")
	}
	log.Printf("Done file load at %v", time.Since(start))

	m.index, err = pdk.SetupPilosa(m.Hosts, m.Index, frames, 1000000)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	if m.ImportGenes {
		err = m.importGeneMasks(genes)
		if err != nil {
			return errors.Wrap(err, "importing genes")
		}
		log.Printf("Imported %d genes", len(genes))
	}

	if m.ImportChromosomes {
		start = time.Now()
		log.Printf("Start chromosomes")
		sliceChan := make(chan uint64, 1000)
		eg := &errgroup.Group{}
		for i := 0; i < m.Concurrency; i++ {
			eg.Go(func() error {
				return m.importChromosomeMasks(sliceChan)
			})
		}
		for s := uint64(0); s < m.maxSlice(); s++ {
			sliceChan <- s
		}
		close(sliceChan)
		err = eg.Wait()
		if err != nil {
			return errors.Wrapf(err, "importing chromosomes")
		}
		log.Printf("Done chromosomes in %v", time.Since(start))
	}

	if m.ImportSequences {
		// Mutator setup.
		rand.Seed(time.Now().UTC().UnixNano())

		for row := uint64(0); row < m.Count; row++ {
			mutationRate := rand.Float64()*(m.Max-m.Min) + m.Min
			start = time.Now()
			log.Printf("Start row %d, mutation rate %f", row, mutationRate)
			sliceChan := make(chan uint64, 1000)
			eg := &errgroup.Group{}
			for i := 0; i < m.Concurrency; i++ {
				var mut Mutator
				if row == 0 {
					mut = NewNopMutator()
				} else {
					mut = NewDeltaMutator(mutationRate)
				}
				eg.Go(func() error {
					return m.importSlices(row, sliceChan, mut)
				})
			}
			for s := uint64(0); s < m.maxSlice(); s++ {
				sliceChan <- s
			}
			close(sliceChan)
			err = eg.Wait()
			if err != nil {
				return errors.Wrapf(err, "importing row %d", row)
			}
			log.Printf("Done row %d in %v", row, time.Since(start))
		}
	}

	return nil
}

func (m *Main) maxSlice() uint64 {
	lastChrom := m.chromosomes[len(m.chromosomes)-1]
	maxCol := lastChrom.offset + uint64(len(lastChrom.data)) - 1
	return BASECOUNT * maxCol / SLICEWIDTH
}

// loadFile loads the data from file into m.chromosomes.
// m.chromosomes = {
//   {
//     data:   long string like "ATGC...",
//     number: int in [1-25],
//     offset: int,
//   }
// }
func (m *Main) loadFile(f string) error {
	file, err := os.Open(f)
	if err != nil {
		return errors.Wrap(err, "opening FASTA file")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	crNumber := 0
	colCount := uint64(0)

	var chr *Chromosome
	var builder strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// HEADER
		if strings.HasPrefix(line, ">") {
			if chr != nil {
				chr.data = builder.String()
			}
			parts := strings.Split(line, " ")
			name := parts[0][1:]
			if !strings.Contains(name, "chr") {
				log.Printf("end of useful info (%v)\n", line)
				break
			}
			crID := name[3:]
			if crID == "X" {
				crNumber = 23
			} else if crID == "Y" {
				crNumber = 24
			} else if crID == "M" {
				crNumber = 25
			} else {
				crNumber, err = strconv.Atoi(crID)
				if err != nil {
					return err
				}
			}
			fmt.Printf("'%v' %v %v %v\n", line, name, crID, crNumber)

			chr = &Chromosome{
				number: crNumber,
				offset: colCount,
			}
			m.chromosomes = append(m.chromosomes, chr)
			builder = strings.Builder{}

			continue
		}

		// LINE
		builder.WriteString(line)
		colCount += uint64(len(line))
	}

	chr.data = builder.String()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}

var errSentinel = errors.New("SENTINEL")

type rangeSlice [][]uint64

func (r rangeSlice) Len() int      { return len(r) }
func (r rangeSlice) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

// Compare ranges by first key of the sub-slice
func (r rangeSlice) Less(i, j int) bool { return r[i][0] < r[j][0] }

func (m *Main) getGenomeSlice(slice uint64) string {

	sw := uint64(SLICEWIDTH / BASECOUNT)

	// make a slice of chromosome position ranges
	var r [][]uint64
	for i, chr := range m.chromosomes {
		r = append(r, []uint64{chr.offset, chr.offset + uint64(len(chr.data)) - 1, uint64(i)})
	}
	sort.Sort(rangeSlice(r))

	posStart := slice * sw
	posEnd := posStart + sw - 1

	var s string
	var chrCount int
	for _, rng := range r {
		if len(s) > 0 {
			if posEnd <= rng[1] {
				return s + m.chromosomes[rng[2]].data[:posEnd-rng[0]+1]
			}
			s += m.chromosomes[rng[2]].data
			chrCount++
			continue
		}
		if posStart >= rng[0] && posStart <= rng[1] {
			// slice is contained in a single chromosome
			if posEnd <= rng[1] {
				return m.chromosomes[rng[2]].data[posStart-rng[0] : posEnd-rng[0]+1]
			}
			if len(s) == 0 {
				s = m.chromosomes[rng[2]].data[posStart-rng[0]:]
				chrCount++
				continue
			}
		}
	}
	return s
}

func (m *Main) importGeneMasks(genes []Gene) error {
	for n, gene := range genes {
		for _, cr := range m.chromosomes {
			if uint64(cr.number) == gene.chromosome {
				for pos := cr.offset + gene.start; pos < cr.offset+gene.end; pos++ {
					m.index.AddBit("genes", BASECOUNT*pos, uint64(n))
					m.index.AddBit("genes", BASECOUNT*pos+1, uint64(n))
					m.index.AddBit("genes", BASECOUNT*pos+2, uint64(n))
					m.index.AddBit("genes", BASECOUNT*pos+3, uint64(n))
				}
				break
			}
		}
		log.Printf("imported gene %+v\n", gene)
	}
	return nil
}

func (m *Main) chromosomeAtColumn(col uint64) (crNumber, length uint64) {
	var r [][]uint64
	for i, chr := range m.chromosomes {
		r = append(r, []uint64{chr.offset, chr.offset + uint64(len(chr.data)) - 1, uint64(i)})
	}
	sort.Sort(rangeSlice(r))

	for i, rr := range r {
		if BASECOUNT*rr[0] < col && col < BASECOUNT*rr[1] {
			crNumber = uint64(i)
			length = BASECOUNT * (rr[1] - col)
		}
	}

	return crNumber, length
}

func (m *Main) importChromosomeMasks(sliceChan chan uint64) error {
	client := m.index.Client()

	for slice := range sliceChan {
		startCol := slice * SLICEWIDTH
		crNumber, length := m.chromosomeAtColumn(startCol)
		next := startCol + length
		rows := make([]uint64, 0, 2097152)
		cols := make([]uint64, 0, 2097152)
		for col := startCol; col < startCol+SLICEWIDTH; col++ {
			rows = append(rows, crNumber)
			cols = append(cols, col)
			if col == next {
				crNumber, length = m.chromosomeAtColumn(col)
				next = col + length
			}
		}

		err := errSentinel
		tries := 0
		for err != nil {
			tries++
			if tries > 10 {
				return err
			}
			err = client.SliceImport(m.Index, "chromosomes", slice, rows, cols)
			if err != nil {
				log.Printf("Error importing slice %d, retrying: %v", slice, err)
			}
		}
		log.Printf("imported chromosome mask for slice %d (%d)", slice, crNumber)
	}
	return nil
}

func (m *Main) importSlices(row uint64, sliceChan chan uint64, mutator Mutator) error {
	client := m.index.Client()

	for slice := range sliceChan {
		startCol := slice * SLICEWIDTH
		bases := m.getGenomeSlice(slice)
		rows := make([]uint64, 0, 2097152)
		cols := make([]uint64, 0, 2097152)
		for i, letter := range bases {
			chr := mutator.mutate(string(letter))
			colz := fastaCodeToNucleotides(chr)
			for _, col := range colz {
				rows = append(rows, row)
				cols = append(cols, startCol+uint64(i)*BASECOUNT+uint64(col))
			}
		}
		err := errSentinel
		tries := 0
		for err != nil {
			tries++
			if tries > 10 {
				return err
			}
			err = client.SliceImport(m.Index, frame, slice, rows, cols)
			if err != nil {
				log.Printf("Error importing slice %d, retrying: %v", slice, err)
			}
		}
	}
	return nil
}
