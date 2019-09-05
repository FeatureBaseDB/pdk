package csv

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/go-pilosa/gpexp"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Main struct {
	Pilosa         []string `help:"Comma separated list of host:port describing Pilosa cluster."`
	Files          []string `help:"File names or URLs to read."`
	Index          string   `help:"Name of index to ingest data into."`
	BatchSize      int      `help:"Number of records to put in a batch before importing to Pilosa."`
	ConfigFile     string   `help:"JSON configuration describing source fields, and how to parse and map them to Pilosa fields."`
	RangeAllocator string   `help:"Designates where to retrieve unused ranged of record IDs (if generating ids). If left blank, generate locally starting from 0."`
	Concurrency    int      `help:"Number of goroutines to run processing files."`

	Config *Config `flag:"-"`
}

func NewMain() *Main {
	return &Main{
		Pilosa:      []string{"localhost:10101"},
		Files:       []string{"data.csv"},
		Index:       "picsvtest",
		BatchSize:   1000,
		Concurrency: 4,

		Config: NewConfig(),
	}
}

func (m *Main) Run() error {
	start := time.Now()

	// Load Config File (if available)
	if m.ConfigFile != "" {
		f, err := os.Open(m.ConfigFile)
		if err != nil {
			return errors.Wrap(err, "opening config file")
		}
		dec := json.NewDecoder(f)
		err = dec.Decode(m.Config)
		if err != nil {
			return errors.Wrap(err, "decoding config file")
		}
	}
	// log.Printf("Flags: %+v\n", *m)
	// log.Printf("Config: %+v\n", *m.Config)

	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		return errors.Wrap(err, "getting pilosa client")
	}
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}
	opts := []pilosa.IndexOption{}
	if m.Config.IDField != "" {
		opts = append(opts, pilosa.OptIndexKeys(true))
	}
	index := schema.Index(m.Index, opts...)
	shardWidth := index.ShardWidth()
	if shardWidth == 0 {
		shardWidth = pilosa.DefaultShardWidth
	}
	// TODO currently ignoring m.RangeAllocator
	ra := pdk.NewLocalRangeAllocator(shardWidth)

	jobs := make(chan fileJob, 0)
	stats := make(chan jobReport, 0)
	eg := errgroup.Group{}
	for i := 0; i < m.Concurrency; i++ {
		eg.Go(func() error {
			fileProcessor(jobs, stats)
			return nil
		})
	}

	totalRecords := uint64(0)
	mu := &sync.Mutex{}
	mu.Lock()
	go func() {
		for stat := range stats {
			log.Printf("processed %s\n", stat)
			totalRecords += stat.n
		}
		mu.Unlock()
	}()

	///////////////////////////////////////////////////////
	// for each file to process
	for _, filename := range m.Files {
		f, err := openFileOrURL(filename)
		if err != nil {
			return errors.Wrapf(err, "opening %s", filename)
		}
		defer f.Close()
		reader := csv.NewReader(f)
		reader.ReuseRecord = true
		reader.FieldsPerRecord = -1

		nexter, err := pdk.NewRangeNexter(ra)
		if err != nil {
			return errors.Wrap(err, "getting nexter")
		}

		batch, parseConfig, err := processHeader(m.Config, client, index, reader, m.BatchSize, nexter)
		if err != nil {
			return errors.Wrap(err, "processing header")
		}
		// this has a non-obvious dependence on processHeader which sets up fields. TODO Do this inside processHeader?
		client.SyncSchema(schema)

		jobs <- fileJob{
			name:   filename,
			reader: reader,
			batch:  batch,
			pc:     parseConfig,
		}
	}
	close(jobs)
	eg.Wait()
	close(stats)
	mu.Lock()
	log.Printf("Processed %d records in %v", totalRecords, time.Since(start))
	return nil
}

type jobReport struct {
	n        uint64
	duration time.Duration
	name     string
	err      error
}

func (j jobReport) String() string {
	s := fmt.Sprintf("{name:%s n:%d duration:%s", j.name, j.n, j.duration)
	if j.err != nil {
		s += fmt.Sprintf(" err:'%s'}", j.err)
	} else {
		s += "}"
	}

	return s
	return fmt.Sprintf("{n:%d duration:%s}", j.n, j.duration)
}

type fileJob struct {
	name   string
	reader *csv.Reader
	batch  *gpexp.Batch
	pc     *parseConfig
}

func fileProcessor(jobs <-chan fileJob, stats chan<- jobReport) {
	for fj := range jobs {
		start := time.Now()
		n, err := processFile(fj.reader, fj.batch, fj.pc)
		stats <- jobReport{
			name:     fj.name,
			n:        n,
			err:      err,
			duration: time.Since(start),
		}
	}
}

func processFile(reader *csv.Reader, batch *gpexp.Batch, pc *parseConfig) (n uint64, err error) {
	defer pc.nexter.Return()
	record := gpexp.Row{
		Values: make([]interface{}, len(pc.fieldConfig)),
	}
	numRecords := uint64(0)
	recsImported := numRecords
	var row []string
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		record.ID, err = pc.getID(row)
		if err != nil {
			return recsImported, errors.Wrap(err, "getting record ID")
		}
		numRecords++
		for _, meta := range pc.fieldConfig {
			if meta.srcIndex > len(row)-1 {
				log.Printf("row: %v\nis not long enough %d is less than %d\n", row, len(row), len(pc.fieldConfig))
			}
			record.Values[meta.recordIndex] = meta.valGetter(row[meta.srcIndex])
		}
		err := batch.Add(record)
		if err == gpexp.ErrBatchNowFull {
			err := batch.Import()
			if err != nil {
				return recsImported, errors.Wrap(err, "importing")
			}
			recsImported = numRecords
		} else if err != nil {
			return recsImported, errors.Wrap(err, "adding to batch")
		}
	}

	if err != io.EOF && err != nil {
		return recsImported, errors.Wrapf(err, "reading csv, record %v", row)
	}
	err = batch.Import()
	if err != nil {
		return recsImported, errors.Wrap(err, "final import")
	}
	recsImported = numRecords
	return recsImported, nil
}

type parseConfig struct {
	getID func(row []string) (interface{}, error)
	// terrible name
	fieldConfig map[string]valueMeta

	r      pdk.IDRange
	nexter pdk.RangeNexter
}

// terrible name
type valueMeta struct {
	srcIndex    int
	recordIndex int
	// terrible name
	valGetter func(val string) interface{}
}

func processHeader(config *Config, client *pilosa.Client, index *pilosa.Index, reader *csv.Reader, batchSize int, nexter pdk.RangeNexter) (*gpexp.Batch, *parseConfig, error) {
	headerRow, err := reader.Read()
	if err != nil {
		return nil, nil, errors.Wrap(err, "reading CSV header")
	}

	pc := &parseConfig{
		fieldConfig: make(map[string]valueMeta),
		nexter:      nexter,
	}
	pc.getID = func(row []string) (interface{}, error) {
		next, err := pc.nexter.Next() // this is kind of weird... wish each fileProcessor had a nexter instead TODO
		if err != nil {
			return nil, err
		}
		return next, nil
	}

	fields := make([]*pilosa.Field, 0, len(headerRow))
	for i, srcFieldName := range headerRow {
		if srcFieldName == config.IDField {
			idIndex := i
			switch config.IDType {
			case "uint64":
				pc.getID = func(row []string) (interface{}, error) {
					uintVal, err := strconv.ParseUint(row[idIndex], 0, 64)
					if err != nil {
						return nil, nil // we don't want to stop because we couldn't parse an ID here... but really it should be up to the caller to determine whether or not it should stop. TODO fix.
					}
					return uintVal, nil
				}
			case "string":
				pc.getID = func(row []string) (interface{}, error) {
					return row[idIndex], nil
				}
			default:
				return nil, nil, errors.Errorf("unknown IDType: %s", config.IDType)
			}
			continue
		}

		var valGetter func(val string) interface{}
		srcField, ok := config.SourceFields[srcFieldName]
		if !ok {
			name := strings.ToLower(srcFieldName)
			name = strings.TrimSpace(name)
			name = strings.ReplaceAll(name, " ", "_")
			// check if there is a normalized version of this name stored
			srcField, ok = config.SourceFields[name]
			// if not, create a new config for it
			if !ok {
				srcField = SourceField{
					TargetField: name,
					Type:        "string",
				}
			}
			config.SourceFields[srcFieldName] = srcField
		}
		pilosaField, ok := config.PilosaFields[srcField.TargetField]
		if !ok {
			pilosaField = Field{
				Type:      "set",
				CacheType: pilosa.CacheTypeRanked,
				CacheSize: 100000,
				Keys:      true,
			}
			config.PilosaFields[srcField.TargetField] = pilosaField
		}

		fieldName := srcField.TargetField
		switch srcField.Type {
		case "ignore":
			continue
		case "int":
			valGetter = func(val string) interface{} {
				if val == "" {
					return nil
				}
				intVal, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					log.Printf("parsing '%s' for %s as int: %v\n", val, fieldName, err)
					return nil
				}
				return intVal
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
		case "float":
			if srcField.Multiplier != 0 {
				valGetter = func(val string) interface{} {
					if val == "" {
						return nil
					}
					floatVal, err := strconv.ParseFloat(val, 64)
					if err != nil {
						log.Printf("parsing '%s' for %s as float: %v\n", val, fieldName, err)
						return nil
					}
					return int64(floatVal * srcField.Multiplier)
				}
			} else {
				valGetter = func(val string) interface{} {
					if val == "" {
						return nil
					}
					floatVal, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return nil
					}
					return int64(floatVal)
				}
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
		case "string":
			valGetter = func(val string) interface{} {
				if val == "" {
					return nil // ignore empty strings
				}
				return val
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
		case "uint64", "rowID":
			valGetter = func(val string) interface{} {
				if val == "" {
					return nil
				}
				uintVal, err := strconv.ParseUint(val, 0, 64)
				if err != nil {
					log.Printf("parsing '%s' for %s as rowID: %v\n", val, fieldName, err)
					return nil
				}
				return uintVal
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
		case "time":
			if srcField.TimeFormat == "" {
				return nil, nil, errors.Errorf("need time format for source field %s of type time", srcFieldName)
			}
			valGetter = func(val string) interface{} {
				if val == "" {
					return nil
				}
				tim, err := time.Parse(srcField.TimeFormat, val)
				if err != nil {
					log.Printf("parsing '%s' for %s as time w/ format '%s': %v\n", val, fieldName, srcField.TimeFormat, err)
					return nil
				}
				return tim.Unix()
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
		default:
			return nil, nil, errors.Errorf("unknown source type '%s'", srcField.Type)
		}
		pc.fieldConfig[fieldName] = valueMeta{
			valGetter:   valGetter,
			srcIndex:    i,
			recordIndex: len(fields) - 1,
		}
	}

	batch, err := gpexp.NewBatch(client, batchSize, index, fields)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting new batch")
	}

	return batch, pc, nil
}

func openFileOrURL(name string) (io.ReadCloser, error) {
	var content io.ReadCloser
	if strings.HasPrefix(name, "http") {
		resp, err := http.Get(name)
		if err != nil {
			return nil, errors.Wrap(err, "getting via http")
		}
		if resp.StatusCode > 299 {
			return nil, errors.Errorf("got status %d via http.Get", resp.StatusCode)
		}
		content = resp.Body
	} else {
		f, err := os.Open(name)
		if err != nil {
			return nil, errors.Wrap(err, "opening file")
		}
		content = f
	}
	return content, nil
}

func NewConfig() *Config {
	return &Config{
		PilosaFields: make(map[string]Field),
		SourceFields: make(map[string]SourceField),
		IDType:       "string",
	}
}

type Config struct {
	PilosaFields map[string]Field       `json:"pilosa-fields"`
	SourceFields map[string]SourceField `json:"source-fields"`

	// IDField denotes which field in the source should be used for Pilosa record IDs.
	IDField string `json:"id-field"`

	// IDType denotes whether the ID field should be parsed as a string or uint64.
	IDType string `json:"id-type"`
}

type Field struct {
	Type      string           `json:"type"`
	Min       int64            `json:"min"`
	Max       int64            `json:"max"`
	Keys      bool             `json:"keys"`
	CacheType pilosa.CacheType `json:"cache-type"`
	CacheSize int              `json:"cache-size"`
	// TODO time stuff
}

func (f Field) MakeOptions() (opts []pilosa.FieldOption) {
	switch f.Type {
	case "set":
		opts = append(opts, pilosa.OptFieldKeys(f.Keys), pilosa.OptFieldTypeSet(f.CacheType, f.CacheSize))
	case "int":
		if f.Max != 0 || f.Min != 0 {
			opts = append(opts, pilosa.OptFieldTypeInt(f.Min, f.Max))
		} else {
			opts = append(opts, pilosa.OptFieldTypeInt())
		}
	default:
		panic(fmt.Sprintf("unknown pilosa field type: %s", f.Type))
	}
	return opts
}

type SourceField struct {
	// TargetField is the Pilosa field that this source field should map to.
	TargetField string `json:"target-field"`

	// Type denotes how the source field should be parsed. (string,
	// int, rowID, float, time, or ignore). rowID means that the field
	// will be parsed as a uint64 and then used directly as a rowID
	// for a set field. If "string", key translation must be on for
	// that Pilosa field, and it must be a set field. If int or float,
	// it must be a Pilosa int field. If time, a TimeFormat should be
	// provided in the Go style using the reference date
	// 2006-01-02T15:04:05Z07:00, and the target field type should be
	// int - time will be stored as the time since Unix epoch in
	// seconds.
	Type string `json:"type"`

	// Multiplier is for float fields. Because Pilosa does not support
	// floats natively, it is sometimes useful to store a float in
	// Pilosa as an integer, but first multiplied by some constant
	// factor to preserve some amount of precision. If 0 this field won't be used.
	Multiplier float64 `json:"multiplier"`
	TimeFormat string  `json:"time-format"`
}

// TODO we should validate the Config once it is constructed.
// What are valid mappings from source fields to pilosa fields?
