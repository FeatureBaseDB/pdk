package taxi

import (
	"math/rand"
)

type userGetter struct {
	z *rand.Zipf
}

func newUserGetter(seed int) *userGetter {
	r := rand.New(rand.NewSource(int64(seed)))
	return &userGetter{
		z: rand.NewZipf(r, 1.1, 1024, 5000000), // zipfian distribution over 5 million users
	}
}

func (u *userGetter) ID() uint64 {
	return u.z.Uint64()
}
