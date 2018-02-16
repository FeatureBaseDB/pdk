package http

import (
	"log"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

type FramerOpts struct {
	Ignore   []string `help:"Do not index paths containing any of these components"`
	Collapse []string `help:"Remove these components from the path before getting frame."`
}

type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

type Main struct {
	Bind        string   `help:"Listen for post requests on this address."`
	PilosaHosts []string `help:"List of host:port pairs for Pilosa cluster."`
	Index       string   `help:"Pilosa index to write to."`
	BatchSize   uint     `help:"Batch size for Pilosa imports."`
	Framer      FramerOpts
	Subjecter   SubjecterOpts
	Proxy       string `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

func NewMain() *Main {
	return &Main{
		Bind:        ":12121",
		PilosaHosts: []string{"localhost:10101"},
		Index:       "jsonhttp",
		BatchSize:   10,
		Framer:      FramerOpts{},
		Subjecter:   SubjecterOpts{},
		Proxy:       ":13131",
	}
}

func (m *Main) Run() error {
	src, err := NewJSONSource(WithAddr(m.Bind))
	if err != nil {
		return errors.Wrap(err, "getting json source")
	}

	log.Println("listening on", src.Addr())

	parser := pdk.NewDefaultGenericParser()
	parser.Subjecter = pdk.SubjectFunc(func(d interface{}) (string, error) {
		dmap, ok := d.(map[string]interface{})
		if !ok {
			return "", errors.Errorf("couldn't get subject from %#v", d)
		}
		next := dmap
		for i, key := range m.Subjecter.Path {
			if i == len(m.Subjecter.Path)-1 {
				val, ok := next[key]
				if !ok {
					return "", errors.Errorf("key #%d:'%s' not found in %#v", i, key, next)
				}
				valStr, ok := val.(string)
				if !ok {
					return "", errors.Errorf("subject value is not a string %#v", val)
				}
				return valStr, nil
			}
			nexti, ok := next[key]
			if !ok {
				return "", errors.Errorf("key #%d:'%s' not found in %#v", i, key, next)
			}
			next, ok = nexti.(map[string]interface{})
			if !ok {
				return "", errors.Errorf("map value of unexpected type %#v", nexti)
			}
		}
		return "", nil // if there are no keys, return blank subject
	})

	mapper := pdk.NewCollapsingMapper()
	mapper.Framer = &pdk.DashFrame{
		Ignore:   m.Framer.Ignore,
		Collapse: m.Framer.Collapse,
	}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := pdk.NewIngester(src, parser, mapper, indexer)
	go func() {
		err = pdk.StartMappingProxy(m.Proxy, pdk.NewPilosaForwarder(m.PilosaHosts[0], mapper.Translator))
		log.Fatal(errors.Wrap(err, "starting mapping proxy"))
	}()
	log.Println("starting ingester")
	return errors.Wrap(ingester.Run(), "running ingester")
}
