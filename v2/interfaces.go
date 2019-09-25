package pdk

// Source is an interface implemented by sources of data which can be
// ingested into Pilosa. Each Record returned from Record is described
// by the slice of Fields returned from Source.Schema directly after
// the call to Source.Record. If the error returned from Source.Record
// is nil, then the call to Schema which applied to the previous
// Record also applies to this Record. Source implementations are
// fundamentally not threadsafe (due to the interplay between Record
// and Schema).
type Source interface {

	// Record returns a data record, and an optional error. If the
	// error is ErrSchemaChange, then the record is valid, but one
	// should call Source.Schema to understand how each of its fields
	// should be interpreted.
	Record() (Record, error)

	// Schema returns a slice of Fields which applies to the most
	// recent Record returned from Source.Record. Every Field has a
	// name and a type, and depending on the concrete type of the
	// Field, may have other information which is relevant to how it
	// should be indexed.
	Schema() []Field
}

type Error string

func (e Error) Error() string { return string(e) }

// ErrSchemaChange is returned from Source.Record when the returned
// record has a different schema from the previous record.
const ErrSchemaChange = Error("this record has a different schema from the previous record (or is the first one delivered). Please call Source.Schema() to fetch the schema in order to properly decode this record")

type Record interface {
	// Commit notifies the Source which produced this record that it
	// and any record which came before it have been completely
	// processed. The Source can then take any necessary action to
	// record which records have been processed, and restart from the
	// earliest unprocessed record in the event of a failure.
	Commit() error

	Data() []interface{}
}

type Field interface {
	Name() string
}

type IDField struct {
	NameVal string

	// Mutex denotes whether we need to enforce that each record only
	// has a single value for this field. Put another way, says
	// whether a new value for this field be treated as adding an
	// additional value, or replacing the existing value (if there is
	// one).
	Mutex bool
}

func (id IDField) Name() string { return id.NameVal }

type BoolField struct {
	NameVal string
}

func (b BoolField) Name() string { return b.NameVal }

type StringField struct {
	NameVal string

	// Mutex denotes whether we need to enforce that each record only
	// has a single value for this field. Put another way, says
	// whether a new value for this field be treated as adding an
	// additional value, or replacing the existing value (if there is
	// one).
	Mutex bool
}

func (s StringField) Name() string { return s.NameVal }

type IntField struct {
	NameVal string
	Min     *int64
	Max     *int64
}

func (i IntField) Name() string { return i.NameVal }

type DecimalField struct {
	NameVal string
	Scale   uint
}

func (d DecimalField) Name() string { return d.NameVal }

type StringArrayField struct {
	NameVal string
}

func (s StringArrayField) Name() string { return s.NameVal }
