package genome

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Main holds the config for the http command.
type Main struct {
	File  string   `help:"Path to FASTA file."`
	Hosts []string `help:"Pilosa hosts."`
	Index string   `help:"Pilosa index."`
	Min   int      `help:"Minimum number of random mutations per [denom]."`
	Max   int      `help:"Maximum number of random mutations per [denom]."`
	Denom int      `help:"Denominator to use for calculating random mutations."`
	Count int      `help:"Number of mutated rows to create."`

	index pdk.Indexer
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Hosts: []string{":10101"},
		Index: "genome",
		Min:   10,
		Max:   50,
		Denom: 10000,
		Count: 10,
	}
}

var frame = "sequences"

var frames = []pdk.FrameSpec{
	pdk.NewRankedFrameSpec(frame, 0),
}

// Run runs the http command.
func (m *Main) Run() error {

	var err error

	log.Println("setting up pilosa")
	m.index, err = pdk.SetupPilosa(m.Hosts, m.Index, frames, 1000000)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	// Mutator setup.
	rand.Seed(time.Now().UTC().UnixNano())

	nopmut, err := NewMutator(0, 1, 1)
	if err != nil {
		return err
	}
	mut, err := NewMutator(m.Min, m.Max, m.Denom)
	if err != nil {
		return err
	}

	err = m.importReferenceWithMutations(nopmut, 0)
	if err != nil {
		return errors.Wrap(err, "importing reference row")
	}
	for row := 1; row < m.Count; row++ {
		mut.setRandomMatch()
		err = m.importReferenceWithMutations(mut, row)
		if err != nil {
			return errors.Wrapf(err, "import row %d", row)
		}
	}

	return nil
}

func (m *Main) importReferenceWithMutations(mutator *Mutator, row int) error {
	gm := NewGenomeMapper(4, chromosomeLengthsPadded)

	file, err := os.Open(m.File)
	if err != nil {
		return errors.Wrap(err, "opening FASTA file")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	positionOnCr := 0
	crNumber := 0
	colCount := 0
	for scanner.Scan() {
		line := scanner.Text()

		// HEADER
		if strings.HasPrefix(line, ">") {
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
			positionOnCr = 0
			fmt.Printf("'%v' %v %v %v\n", line, name, crID, crNumber)
			continue
		}

		// LINE
		for _, c := range line {
			char := string(c)
			// Random mutation.
			char = mutator.mutate(char)

			nucleotides := fastaCodeToNucleotides(char)
			for _, nuc := range nucleotides {
				col := gm.positionToColumn(crNumber, positionOnCr, nuc)
				if col%1000000 == 0 {
					fmt.Printf("row: %d, col: %d\n", row, col)
				}
				m.index.AddBit(frame, uint64(col), uint64(row))
				colCount++
			}
			positionOnCr++
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}
