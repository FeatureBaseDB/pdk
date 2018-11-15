package fake

import (
	"io"
	"math"
	"math/rand"
	"sync/atomic"

	"github.com/pilosa/pdk/fake/gen"
)

// UserSource is a pdk.Source which generates fake user data.
type UserSource struct {
	max uint64
	n   *uint64
	ug  *UserGenerator
}

// NewUserSource creates a new Source with the given random seed. Using the same
// seed should give the same series of events on a given version of Go.
func NewUserSource(seed int64, max uint64) *UserSource {
	if max == 0 {
		max = math.MaxUint64
	}
	var n uint64
	s := &UserSource{
		n:   &n,
		max: max,
		ug:  NewUserGenerator(seed),
	}

	return s
}

// Record implements pdk.Source and returns a randomly generated user.
func (s *UserSource) Record() (interface{}, error) {
	next := atomic.AddUint64(s.n, 1)
	if next > s.max {
		return nil, io.EOF
	}
	user := s.ug.Record()
	user.ID = next - 1
	return user, nil
}

// User is a theoretical user type.
type User struct {
	ID        uint64
	Age       int
	FirstName string
	LastName  string
	Allergies []string
	Title     string
}

// UserGenerator generates fake Users.
type UserGenerator struct {
	g *gen.Generator
	r *rand.Rand
}

// NewUserGenerator initializes a new UserGenerator.
func NewUserGenerator(seed int64) *UserGenerator {
	return &UserGenerator{
		g: gen.NewGenerator(seed),
		r: rand.New(rand.NewSource(seed)),
	}
}

// Record returns a random User record with realistic-ish values.
func (u *UserGenerator) Record() *User {
	return &User{
		Age:       u.r.Intn(110),
		FirstName: u.g.String(8, 10000),
		LastName:  u.g.String(10, 50000),
		Allergies: u.genAllergies(),
		Title:     titleList[u.g.Uint64(len(titleList))],
	}
}

func (u *UserGenerator) genAllergies() []string {
	n := u.g.Uint64(len(allergyList) - 1)
	nums := u.r.Perm(len(allergyList))
	allergies := make([]string, n)
	for i := 0; i < int(n); i++ {
		allergies[i] = allergyList[nums[i]]
	}
	return allergies
}

var allergyList = []string{"Balsam of Peru", "Egg", "Fish", "Shellfish", "Fruit", "Garlic", "Hot Peppers", "Oats", "Meat", "Milk", "Peanut", "Rice", "Sesame", "Soy", "Sulfites", "Tartrazine", "Tree Nut", "Wheat", "Tetracycline", "Dilantin", "Tegretol", "Penicillin", "Cephalosporins", "Sulfonamides", "Cromolyn", "Sodium", "Nedocromil", "Pollen", "Cat", "Dog", "Insect Sting", "Mold", "Perfume", "Cosmetics", "Latex", "Water", "Nickel", "Gold", "Chromium", "Cobalt Chloride", "Formaldehyde", "Photographic Developers", "Fungicide"}

var titleList = []string{"Specialist", "Director", "Designer", "Analyst", "Consultant", "Manager", "Assistant", "Copywriter", "Strategist", "VP", "Executive", "QC", "CEO", "HR", "Receptionist", "Secretary", "Clerk", "Auditor", "Bookkeeper", "Data Entry", "Computer Scientist", "IT Professional", "UX Designer", "SQL Developer", "Web Developer", "Software Engineer", "DevOps Engineer", "Computer Programmer", "Network Administrator", "Information Security Analyst", "Artificial Intelligence Engineer", "Cloud Architect", "IT Manager", "Technical Specialist", "Application Developer", "CTO", "CIO"}
