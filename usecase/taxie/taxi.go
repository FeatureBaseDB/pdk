package taxie

import (
	"bufio"
	"io"
	"log"
	"os"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/csv"
	"github.com/pilosa/pdk/enterprise"
	"github.com/pkg/errors"

	"net/http"
	_ "net/http/pprof"
	"runtime"
)

type Main struct {
	Pilosa  []string `help:"Pilosa cluster: comma separated list of host:port."`
	URLFile string   `help:"File containing URLs of taxi data CSVs - can be local or http urls."`
	Index   string   `help:"Pilosa index to import into."`
}

func NewMain() *Main {
	return &Main{
		Pilosa:  []string{"localhost:20202"}, // enterprise default
		URLFile: "greenAndYellowUrls.txt",
		Index:   "taxie",
	}
}

func (m *Main) Run() error {
	runtime.SetBlockProfileRate(1000)
	runtime.SetMutexProfileFraction(1000)
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	f, err := os.Open(m.URLFile)
	if err != nil {
		return errors.Wrap(err, "opening url file")
	}
	urls, err := getURLs(f)
	if err != nil {
		return errors.Wrap(err, "getting URLs")
	}

	err = f.Close()
	if err != nil {
		return errors.Wrap(err, "closing url file")
	}

	src := csv.NewCSVSource(urls)
	parser := pdk.NewDefaultGenericParser()
	parser.Subjecter = pdk.SubjectFunc(func(d interface{}) (string, error) {
		dm, ok := d.(map[string]interface{})
		if !ok {
			return "", errors.Errorf("expected map, but got : %#v", d)
		}
		subj, ok := dm[","].(string)
		if !ok {
			return "", errors.Errorf("expected ',' key, but: %#v", dm)
		}
		delete(dm, ",")
		return subj, nil
	})
	indexer, err := enterprise.SetupIndex(m.Pilosa, m.Index, nil, 100000)
	if err != nil {
		return errors.Wrap(err, "setting up index")
	}
	ingester := enterprise.NewIngester(src, parser, indexer)
	return ingester.Run()
}

func getURLs(r io.Reader) ([]string, error) {
	scan := bufio.NewScanner(r)
	urls := make([]string, 0)
	for scan.Scan() {
		urls = append(urls, scan.Text())
	}
	return urls, errors.Wrap(scan.Err(), "scanner ")
}
