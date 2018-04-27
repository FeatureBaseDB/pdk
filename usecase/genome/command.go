package genome

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Main holds the config for the http command.
type Main struct {
	File  string `help:"Path to FASTA file."`
	Min   int    `help:"min"`
	Max   int    `help:"max"`
	Denom int    `help:"denom"`
	Count int    `help:"count"`
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Min:   10,
		Max:   50,
		Denom: 10000,
		Count: 10,
	}
}

// Run runs the http command.
func (m *Main) Run() error {
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
	for scanner.Scan() {
		line := scanner.Text()

		// HEADER
		if strings.HasPrefix(line, ">") {
			// new section of file
			parts := strings.Split(line, " ")
			name := parts[0][1:]
			crNumber++
			positionOnCr = 0
			fmt.Println(name)
			if strings.Contains(name, "_") {
				// done with normal chromosome sections
				break
			}
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
				fmt.Printf("%d %d %s %v\n", crNumber, positionOnCr, char, col)
			}
			positionOnCr++
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}
