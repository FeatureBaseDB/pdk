package pdk

import (
	"time"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type FrameSpec struct {
	Name           string
	CacheType      pcli.CacheType
	CacheSize      uint
	InverseEnabled bool
}

const DefaultCacheSize = 50000

func NewFrameSpec(name string) FrameSpec {
	return FrameSpec{
		Name:      name,
		CacheType: pcli.CacheTypeRanked,
		CacheSize: DefaultCacheSize,
	}
}

func SetupPilosa(hosts []string, index string, frames []FrameSpec) (*pcli.Client, error) {
	client, err := pcli.NewClientFromAddresses(hosts,
		&pcli.ClientOptions{SocketTimeout: time.Minute * 60,
			ConnectTimeout: time.Second * 60,
		})
	if err != nil {
		return nil, errors.Wrap(err, "creating pilosa cluster client")
	}

	idx, err := pcli.NewIndex(index, &pcli.IndexOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "making index")
	}
	err = client.EnsureIndex(idx)
	if err != nil {
		return nil, errors.Wrap(err, "ensuring index existence")
	}
	for _, frame := range frames {
		fram, err := idx.Frame(frame.Name,
			&pcli.FrameOptions{CacheType: frame.CacheType, CacheSize: frame.CacheSize})
		if err != nil {
			return nil, errors.Wrap(err, "making frame: %v")
		}
		err = client.EnsureFrame(fram)
		if err != nil {
			return nil, errors.Wrapf(err, "creating frame '%v': %v", frame)
		}
	}

	return client, nil

}
