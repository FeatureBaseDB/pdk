package genome

import (
	"sort"
	"strings"
)

// rounded up to the next 10M - fits both ref37 and ref38, and hopefully any other versions
var chromosomeLengthsPadded = []int{250000000, 250000000, 200000000, 200000000, 190000000, 180000000, 160000000, 150000000, 150000000, 140000000, 140000000, 140000000, 120000000, 110000000, 110000000, 100000000, 90000000, 90000000, 60000000, 70000000, 50000000, 60000000, 160000000, 60000000, 10000000}

// https://en.wikipedia.org/wiki/Human_genome#Molecular_organization_and_gene_content
// var chromosomeLengthsV38 = [...]int{248956422, 242193529, 198295559, 190214555, 181538259, 170805979, 159345973, 145138636, 138394717, 133797422, 135086622, 133275309, 114364328, 107043718, 101991189, 90338345, 83257441, 80373285, 58617616, 64444167, 46709983, 50818468, 156040895, 57227415}
// var chromosomeLengthsV38Cumulative = []int{0, 248956422, 491149951, 689445510, 879660065, 1061198324, 1232004303, 1391350276, 1536488912, 1674883629, 1808681051, 1943767673, 2077042982, 2191407310, 2298451028, 2400442217, 2490780562, 2574038003, 2654411288, 2713028904, 2777473071, 2824183054, 2875001522, 3031042417}

// GenomeMapper represents the mapping from specific positions in the genome
// to Pilosa columns. Computed deterministically, based on a specific reference genome version.
type GenomeMapper struct {
	columnsPerPosition          int
	chromosomeLengthsCumulative []int
}

// NewGenomeMapper creates a new GenomeMapper based on a provided list of chromosome lengths.
func NewGenomeMapper(columnsPerPosition int, chromosomeLengths []int) GenomeMapper {
	a := make([]int, 0, len(chromosomeLengthsPadded))
	accum := 0
	for _, l := range chromosomeLengths {
		a = append(a, accum)
		accum += l
	}
	return GenomeMapper{
		chromosomeLengthsCumulative: a,
		columnsPerPosition:          columnsPerPosition,
	}
}

// positionToColumn maps a general (chromosome, position, nucleotide) tuple to
// a single Pilosa column. [a, c, g, t] -> [0, 1, 2, 3].
func (m GenomeMapper) positionToColumn(crNumber, position, nucleotideNum int) int {
	crStart := m.chromosomeLengthsCumulative[crNumber-1] // chromosomes are 1-based
	return m.columnsPerPosition*(crStart+position) + nucleotideNum
}

// fastaCodeToColumns maps a fasta-style (chromosome, position, code) tuple to a
// set of nucleotides. [a, c, g, t] -> [0, 1, 2, 3].
func fastaCodeToNucleotides(code string) []int {
	return fastaCharMap[strings.ToLower(code)]
}

// https://en.wikipedia.org/wiki/FASTA_format#Sequence_representation
var fastaCharMap = map[string][]int{
	"a": []int{0},
	"c": []int{1},
	"g": []int{2},
	"t": []int{3},
	"u": []int{}, // skip uracil and anything uncertain
	"r": []int{},
	"y": []int{},
	"k": []int{},
	"m": []int{},
	"s": []int{},
	"w": []int{},
	"b": []int{},
	"d": []int{},
	"h": []int{},
	"v": []int{},
	"n": []int{}, // N -> any nucleic acid = no information
	"-": []int{}, // gap of indeterminate length = error

	// map to multiple columns
	/*
		"r": []int{0, 1},
		"y": []int{1, 2},
		"k": []int{2, 3},
		"m": []int{0, 1},
		"s": []int{1, 2},
		"w": []int{0, 3},
		"b": []int{1, 2, 3},
		"d": []int{0, 2, 3},
		"h": []int{0, 1, 3},
		"v": []int{0, 1, 2},
		"n": []int{}, // N -> any nucleic acid = no information
		"-": []int{}, // gap of indeterminate length = error
	*/
}

type rangeSlice [][]uint64

func (r rangeSlice) Len() int      { return len(r) }
func (r rangeSlice) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

// Compare ranges by first key of the sub-slice
func (r rangeSlice) Less(i, j int) bool { return r[i][0] < r[j][0] }

func getGenomeSlice(slice uint64, chroms []*Chromosome) string {

	// make a slice of chromosome position ranges
	var r [][]uint64
	for i, chr := range chroms {
		r = append(r, []uint64{chr.offset, chr.offset + uint64(len(chr.data)) - 1, uint64(i)})
	}
	sort.Sort(rangeSlice(r))

	posStart := slice * SLICEWIDTH
	posEnd := posStart + SLICEWIDTH - 1

	var s string
	var chrCount int
	for _, rng := range r {
		if len(s) > 0 {
			if posEnd <= rng[1] {
				return s + chroms[rng[2]].data[:posEnd-rng[0]+1]
			} else {
				s += chroms[rng[2]].data
				chrCount++
				continue
			}
		}
		if posStart >= rng[0] && posStart <= rng[1] {
			// slice is contained in a single chromosome
			if posEnd <= rng[1] {
				return chroms[rng[2]].data[posStart-rng[0] : posEnd-rng[0]+1]
			}
			if len(s) == 0 {
				s = chroms[rng[2]].data[posStart-rng[0]:]
				chrCount++
				continue
			}
		}
	}
	return s
}
