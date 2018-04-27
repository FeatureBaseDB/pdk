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
	File      string `help:"Path to FASTA file."`
	Reference bool   `help:"import reference genome (no mutations)"`
	Min       int    `help:"min"`
	Max       int    `help:"max"`
	Denom     int    `help:"denom"`
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Reference: false,
		Min:       1,
		Max:       5,
		Denom:     100,
	}
}

// Run runs the http command.
func (m *Main) Run() error {

	// Mutator setup.
	rand.Seed(time.Now().UTC().UnixNano())

	mut, err := NewMutator(m.Min, m.Max, m.Denom)
	if err != nil {
		return err
	}

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
			if !m.Reference {
				char = mut.mutate(char)
			}

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
