package csv

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/go-pilosa/gpexp"
	"github.com/pkg/errors"
)

type Main struct {
	Pilosa     []string
	File       string
	Index      string
	BatchSize  int
	ConfigFile string

	Config *Config `flag:"-"`
}

func NewMain() *Main {
	return &Main{
		Pilosa:    []string{"localhost:10101"},
		File:      "data.csv",
		Index:     "picsvtest",
		BatchSize: 1000,

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
	log.Printf("Flags: %+v\n", *m)
	log.Printf("Config: %+v\n", *m.Config)

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

	///////////////////////////////////////////////////////
	// for each file to process (just one right now)
	f, err := os.Open(m.File)
	if err != nil {
		return errors.Wrap(err, "opening file")
	}
	defer f.Close()
	reader := csv.NewReader(f)

	batch, parseConfig, err := processHeader(m.Config, client, index, reader, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "processing header")
	}
	// this has a non-obvious dependence on processHeader which sets up fields. TODO Do this inside processHeader?
	client.SyncSchema(schema)

	// TODO send actual file processing to worker pool.
	num, err := processFile(reader, batch, parseConfig)
	if err != nil {
		return errors.Wrapf(err, "processing %s", f.Name())
	}
	log.Printf("Num: %d, Duration: %s", num, time.Since(start))
	return nil
}

func processFile(reader *csv.Reader, batch *gpexp.Batch, pc *parseConfig) (n uint64, err error) {
	record := gpexp.Row{
		Values: make([]interface{}, len(pc.fieldConfig)),
	}
	numRecords := uint64(0)
	recsImported := numRecords
	var row []string
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		record.ID = pc.getID(row, numRecords)
		numRecords++
		for _, meta := range pc.fieldConfig {
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
	getID func(row []string, numRecords uint64) interface{}
	// terrible name
	fieldConfig map[string]valueMeta
}

// terrible name
type valueMeta struct {
	srcIndex    int
	recordIndex int
	// terrible name
	valGetter func(val string) interface{}
}

func processHeader(config *Config, client *pilosa.Client, index *pilosa.Index, reader *csv.Reader, batchSize int) (*gpexp.Batch, *parseConfig, error) {
	headerRow, err := reader.Read()
	if err != nil {
		return nil, nil, errors.Wrap(err, "reading CSV header")
	}

	pc := &parseConfig{
		fieldConfig: make(map[string]valueMeta),
	}
	pc.getID = func(row []string, numRecords uint64) interface{} {
		return numRecords
	}

	fields := make([]*pilosa.Field, 0, len(headerRow))
	for i, fieldName := range headerRow {
		if fieldName == config.IDField {
			idIndex := i
			switch config.IDType {
			case "uint64":
				pc.getID = func(row []string, numRecords uint64) interface{} {
					uintVal, err := strconv.ParseUint(row[idIndex], 0, 64)
					if err != nil {
						return nil
					}
					return uintVal
				}
			case "string":
				pc.getID = func(row []string, numRecords uint64) interface{} {
					return row[idIndex]
				}
			default:
				return nil, nil, errors.Errorf("unknown IDType: %s", config.IDType)
			}
			continue
		}

		var valGetter func(val string) interface{}
		srcField, ok := config.SourceFields[fieldName]
		if !ok {
			srcField = SourceField{
				TargetField: fieldName,
				Type:        "string",
			}
			config.SourceFields[fieldName] = srcField
		}
		pilosaField, ok := config.PilosaFields[srcField.TargetField]
		if !ok {
			pilosaField = Field{
				Type:      "set",
				CacheType: pilosa.CacheTypeRanked,
				CacheSize: 100000,
				Keys:      true,
			}
			config.PilosaFields[fieldName] = pilosaField
		}

		fieldName = srcField.TargetField
		switch srcField.Type {
		case "ignore":
			continue
		case "int":
			valGetter = func(val string) interface{} {
				intVal, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return nil
				}
				return intVal
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
		case "float":
			if srcField.Multiplier != 0 {
				valGetter = func(val string) interface{} {
					floatVal, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return nil
					}
					return int64(floatVal * srcField.Multiplier)
				}
			} else {
				valGetter = func(val string) interface{} {
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
		case "uint64":
			valGetter = func(val string) interface{} {
				uintVal, err := strconv.ParseUint(val, 0, 64)
				if err != nil {
					return nil
				}
				return uintVal
			}
			fields = append(fields, index.Field(fieldName, pilosaField.MakeOptions()...))
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
	// int, rowID, float, or ignore). rowID means that the field will
	// be parsed as a uint64 and then used directly as a rowID for a
	// set field. If "string", key translation must be on for that
	// Pilosa field, and it must be a set field. If int or float, it
	// must be a Pilosa int field.
	Type string `json:"type"`

	// Multiplier is for float fields. Because Pilosa does not support
	// floats natively, it is sometimes useful to store a float in
	// Pilosa as an integer, but first multiplied by some constant
	// factor to preserve some amount of precision. If 0 this field won't be used.
	Multiplier float64 `json:"multiplier"`
}

// TODO we should validate the Config once it is constructed.
// What are valid mappings from source fields to pilosa fields?
