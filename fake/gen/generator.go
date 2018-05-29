package gen

import (
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"hash"
	"math/rand"
	"sync"
	"time"
)

// Generator holds state for generating random data in certain distributions.
type Generator struct {
	r     *rand.Rand
	zs    map[int]*rand.Zipf
	times map[time.Time]time.Duration
	hsh   hash.Hash
}

// NewGenerator gets a new Generator
func NewGenerator(seed int64) *Generator {
	r := rand.New(rand.NewSource(seed))
	return &Generator{
		r:     r,
		zs:    make(map[int]*rand.Zipf),
		times: make(map[time.Time]time.Duration),
		hsh:   sha1.New(),
	}
}

// String gets a zipfian random string from a set with the given cardinality.
func (g *Generator) String(length, cardinality int) string {
	if length > 32 {
		length = 32
	}

	val := g.Uint64(cardinality)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	_, _ = g.hsh.Write(b) // no need to check err
	hashed := g.hsh.Sum(nil)
	g.hsh.Reset()
	return base32.StdEncoding.EncodeToString(hashed)[:length]
}

// Uint64 gets a zipfian random uint64 with the given cardinality.
func (g *Generator) Uint64(cardinality int) uint64 {
	z, ok := g.zs[cardinality]
	if !ok {
		// We subtract one from cardinality because rand.Zipf generates values
		// in [0, imax], but the expectation from funcs like rand.Intn is to
		// generate values in [0, n). Also since we can generate 0, this means
		// that the actual cardinality of the values we can return matches
		// "cardinality".
		imax := uint64(cardinality) - 1
		v := 0.05 * float64(imax)
		if v < 1.0 {
			v = 1.0
		}
		z = rand.NewZipf(g.r, 1.1, v, imax)
		g.zs[cardinality] = z
	}
	return z.Uint64()
}

// Time returns a time increasing from the "from" time with a random delta.
func (g *Generator) Time(from time.Time, maxDelta time.Duration) time.Time {
	delta, ok := g.times[from]
	if !ok {
		delta = time.Duration(g.r.Uint64() % uint64(maxDelta))
		g.times[from] = delta
	} else {
		delta += time.Duration(g.r.Uint64() % uint64(maxDelta))
		g.times[from] = delta
	}
	return from.Add(delta)
}

// Global convenience funcs

var globalGen = NewGenerator(0)
var globalLk = sync.Mutex{}

// String returns a random string with the given length (<=32), from a set of
// possible strings of size "cardinality".
func String(length, cardinality int) string {
	globalLk.Lock()
	defer globalLk.Unlock()
	return globalGen.String(length, cardinality)
}

// Uint64 gets a zipfian random uint64 with the given cardinality.
func Uint64(cardinality int) uint64 {
	globalLk.Lock()
	defer globalLk.Unlock()
	return globalGen.Uint64(cardinality)
}

// Time returns a time increasing from the "from" time with a random delta.
func Time(from time.Time, maxDelta time.Duration) time.Time {
	globalLk.Lock()
	defer globalLk.Unlock()
	return globalGen.Time(from, maxDelta)
}

// Permutation stuff

// PermutationGenerator provides a way to pass integer IDs through a permutation
// map that is pseudorandom but repeatable. This could be done with rand.Perm,
// but that would require storing a [Iterations]int64 array, which we want to avoid
// for large values of Iterations.
// It works by using a Linear Congruence Generator (https://en.wikipedia.org/wiki/Linear_congruential_generator)
// with modulus m = Iterations,
// c = an arbitrary prime,
// a = computed to ensure the full period.
// relevant stackoverflow: http://cs.stackexchange.com/questions/29822/lazily-computing-a-random-permutation-of-the-positive-integers
type PermutationGenerator struct {
	a int64
	c int64
	m int64
}

// NewPermutationGenerator returns a PermutationGenerator which will permute
// numbers in 0-n.
func NewPermutationGenerator(m int64, seed int64) *PermutationGenerator {
	// figure out 'a' and 'c', return PermutationGenerator
	a := multiplierFromModulus(m, seed)
	c := int64(22695479)
	return &PermutationGenerator{a, c, m}
}

// Permute gets the permuted value for n.
func (p *PermutationGenerator) Permute(n int64) int64 {
	// run one step of the LCG
	return (n*p.a + p.c) % p.m
}

// LCG parameters must satisfy three conditions:
// 1. m and c are relatively prime (satisfied for prime c != m)
// 2. a-1 is divisible by all prime factors of m
// 3. a-1 is divisible by 4 if m is divisible by 4
// Additionally, a seed can be used to select between different permutations
func multiplierFromModulus(m int64, seed int64) int64 {
	factors := primeFactors(m)
	product := int64(1)
	for p := range factors {
		// satisfy condition 2
		product *= p
	}

	if m%4 == 0 {
		// satisfy condition 3
		product *= 2
	}

	return product*seed + 1
}

// Returns map of {integerFactor: count, ...}
// This is a naive algorithm that will not work well for large prime n.
func primeFactors(n int64) map[int64]int {
	factors := make(map[int64]int)
	for i := int64(2); i <= n; i++ {
		div, mod := n/i, n%i
		for mod == 0 {
			factors[i]++
			n = div
			div, mod = n/i, n%i
		}
	}
	return factors
}
