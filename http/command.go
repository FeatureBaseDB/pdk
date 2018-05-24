package http

import (
	"log"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Main holds the config for the http command.
type Main struct {
	Bind           string   `help:"Listen for post requests on this address."`
	PilosaHosts    []string `help:"List of host:port pairs for Pilosa cluster."`
	Index          string   `help:"Pilosa index to write to."`
	BatchSize      uint     `help:"Batch size for Pilosa imports. Default: 100000"`
	ImportStrategy string   `help:"Import strategy. One of 'batch' or 'timeout'. Default: batch"`
	ThreadCount    uint     `help:"Number of import workers. Default: 1"`
	ImportTimeout  uint     `help:"Timeout in milliseconds for the import strategy. Default: 100"`
	Framer         pdk.DashFrame
	Subjecter      SubjecterOpts
	Proxy          string `help:"Bind to this address to proxy and translate requests to Pilosa"`
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Bind:           ":12121",
		PilosaHosts:    []string{"localhost:10101"},
		Index:          "jsonhttp",
		BatchSize:      100000,
		ImportStrategy: "batch",
		ThreadCount:    1,
		ImportTimeout:  200,
		Framer:         pdk.DashFrame{},
		Subjecter:      SubjecterOpts{},
		Proxy:          ":13131",
	}
}

// Run runs the http command.
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
	mapper.Framer = &m.Framer

	importOptions := []gopilosa.ImportOption{}

	if m.BatchSize < 1 {
		return errors.New("Batch size should be greater than 0")
	}
	importOptions = append(importOptions, gopilosa.OptImportBatchSize(int(m.BatchSize)))

	if m.ImportTimeout < 1 {
		return errors.New("Import timeout should be greater than 0")
	}
	importOptions = append(importOptions, gopilosa.OptImportTimeout(time.Duration(m.ImportTimeout)*time.Millisecond))

	if m.ThreadCount < 1 {
		return errors.New("Number of import workers should be greater than 0")
	}
	importOptions = append(importOptions, gopilosa.OptImportThreadCount(int(m.ThreadCount)))

	switch m.ImportStrategy {
	case "batch":
		importOptions = append(importOptions, gopilosa.OptImportStrategy(gopilosa.BatchImport))
	case "timeout":
		importOptions = append(importOptions, gopilosa.OptImportStrategy(gopilosa.TimeoutImport))
	default:
		return errors.New("Import strategy should be one of: batch, timeout")
	}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, importOptions...)
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
