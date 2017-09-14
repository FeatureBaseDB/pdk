package ssb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

type Main struct {
	Dir             string
	Hosts           []string
	Index           string
	SFHint          int
	ReadConcurrency int // TODO tweak for perf
	MapConcurrency  int // TODO tweak for perf

	trans  pdk.Translator
	index  pdk.Indexer
	nexter pdk.Nexter
}

func NewMain() (*Main, error) {
	trans, err := NewTranslator("ssdbmapping")
	if err != nil {
		return nil, err
	}
	return &Main{
		Index:           "ssb",
		ReadConcurrency: 1,
		MapConcurrency:  1,

		nexter: pdk.NewNexter(),
		trans:  trans,
	}, nil
}

func (m *Main) Run() (err error) {
	log.Println("setting up pilosa")
	m.index, err = pdk.SetupPilosa(m.Hosts, m.Index, frames)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	log.Println("reading in edge tables.")
	custs, parts, supps, dates, err := m.setupEdgeTables()
	if err != nil {
		return errors.Wrap(err, "setting up edge tables")
	}

	log.Println("reading lineorder table.")
	rc := make(chan *record, 0) // TODO tweak for perf

	go func() {
		m.runReaders(rc, custs, parts, supps, dates)
		m.index.Close() // close import channels
	}()

	log.Println("running mappers")
	m.runMappers(rc)

	log.Println("mappers finished - starting proxy")
	return pdk.StartMappingProxy("localhost:3456", "localhost:10101", m.trans)
}

func (m *Main) runMappers(rc <-chan *record) {
	wg := sync.WaitGroup{}
	for i := 0; i < m.MapConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.mapRecords(rc)
		}()
	}
	wg.Wait()
}

func (m *Main) mapRecords(rc <-chan *record) {
	for rec := range rc {
		col := m.nexter.Next()

		id, err := m.trans.GetID("lo_year", rec.order_year)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("lo_year", col, id)
		id, err = m.trans.GetID("lo_month", rec.order_month)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("lo_month", col, id)

		id, err = m.trans.GetID("lo_weeknum", rec.order_weeknum)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("lo_weeknum", col, id)

		m.index.AddValue("lo_quantity", col, uint64(rec.lo_quantity))
		m.index.AddValue("lo_extendedprice", col, uint64(rec.lo_extendedprice))
		m.index.AddValue("lo_discount", col, uint64(rec.lo_discount))
		m.index.AddValue("lo_revenue", col, uint64(rec.lo_revenue))
		m.index.AddValue("lo_supplycost", col, uint64(rec.lo_supplycost))

		id, err = m.trans.GetID("c_city", rec.c_city)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("c_city", col, id)
		id, err = m.trans.GetID("c_nation", rec.c_nation)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("c_nation", col, id)

		id, err = m.trans.GetID("c_region", rec.c_region)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("c_region", col, id)

		id, err = m.trans.GetID("s_city", rec.s_city)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("s_city", col, id)
		id, err = m.trans.GetID("s_nation", rec.s_nation)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("s_nation", col, id)

		id, err = m.trans.GetID("s_region", rec.s_region)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("s_region", col, id)

		id, err = m.trans.GetID("p_mfgr", rec.p_mfgr)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("p_mfgr", col, id)

		id, err = m.trans.GetID("p_category", rec.p_category)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("p_category", col, id)

		id, err = m.trans.GetID("p_brand1", rec.p_brand1)
		if err != nil {
			log.Printf("Couldn't map record col: %v, rec: %v, err: %v", col, rec, err)
		}
		m.index.AddBit("p_brand1", col, id)

		// TODO add computed fields - profit and revenue_computed
	}
}

type record struct {
	// TODO add lo orderkey and linenumber so we can store column/key mapping?
	lo_quantity      uint8
	lo_extendedprice uint16
	lo_discount      uint8
	lo_revenue       uint16
	lo_supplycost    uint32

	c_city   []byte
	c_nation []byte
	c_region []byte

	s_city   []byte
	s_nation []byte
	s_region []byte

	p_mfgr     []byte
	p_category []byte
	p_brand1   []byte

	order_year    uint16
	order_month   []byte
	order_weeknum uint8
}

func (r *record) String() string {
	return fmt.Sprintf("{year: %d, month: %s, week: %d, quant: %d, extp: %d, disc: %d, rev: %d, suppcost: %d, c_city: %s, c_nation: %s, c_region: %s, s_city: %s, s_nation: %s, s_region: %s, p_mfgr: %s, p_category: %s, p_brand1: %s}", r.order_year, r.order_month, r.order_weeknum, r.lo_quantity, r.lo_extendedprice, r.lo_discount, r.lo_revenue, r.lo_supplycost, r.c_city, r.c_nation, r.c_region, r.s_city, r.s_nation, r.s_region, r.p_mfgr, r.p_category, r.p_brand1)
}

func (m *Main) runReaders(rc chan<- *record, custs map[int]customer, parts map[int]part, supps map[int]supplier, dates map[int]date) error {
	fil, err := os.Open(m.Dir + "/lineorder.tbl")
	if err != nil {
		return errors.Wrap(err, "opening lineorder.tbl")
	}
	frags, err := pdk.SplitFileLines(fil, int64(m.ReadConcurrency))
	if err != nil {
		return errors.Wrap(err, "splitting file")
	}
	wg := sync.WaitGroup{}
	for _, frag := range frags {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parseLineOrder(frag, rc, custs, parts, supps, dates)
		}()
	}
	wg.Wait()
	return nil
}

func parseLineOrder(r io.Reader, rc chan<- *record, custs map[int]customer, parts map[int]part, supps map[int]supplier, dates map[int]date) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := bytes.Split(scanner.Bytes(), []byte{'|'})
		custkey, err := strconv.Atoi(string(line[2]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting cust key to int: %v", line, err)
			continue
		}
		partkey, err := strconv.Atoi(string(line[3]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting supplier key to int: %v", line, err)
			continue
		}
		suppkey, err := strconv.Atoi(string(line[4]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting part key to int: %v", line, err)
			continue
		}
		datekey, err := strconv.Atoi(string(line[5]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting date key to int: %v", line, err)
			continue
		}
		cust, okcust := custs[custkey]
		par, okpar := parts[partkey]
		supp, oksupp := supps[suppkey]
		dat, okdat := dates[datekey]
		if !(okcust && okpar && oksupp && okdat) {
			log.Printf("FK lookup fail: %v:%v, %v:%v, %v:%v, %v:%v", custkey, okcust, partkey, okpar, suppkey, oksupp, datekey, okdat)
			continue
		}
		quantity, err := strconv.Atoi(string(line[8]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting quantity to int: %v", line, err)
			continue
		}
		extendedprice, err := strconv.Atoi(string(line[9]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting extendedprice to int: %v", line, err)
			continue
		}
		discount, err := strconv.Atoi(string(line[11]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting discount to int: %v", line, err)
			continue
		}
		revenue, err := strconv.Atoi(string(line[12]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting revenue to int: %v", line, err)
			continue
		}
		supplycost, err := strconv.Atoi(string(line[13]))
		if err != nil {
			log.Printf("Lineorder line %v. Converting supplycost to int: %v", line, err)
			continue
		}
		if bytes.Contains(dat.month, []byte(",")) {
			log.Printf("got bad date for lineorder: %s, date: %v", line, dat)
		}

		rc <- &record{
			lo_quantity:      uint8(quantity),
			lo_extendedprice: uint16(extendedprice),
			lo_discount:      uint8(discount),
			lo_revenue:       uint16(revenue),
			lo_supplycost:    uint32(supplycost),

			c_city:   cust.city,
			c_nation: cust.nation,
			c_region: cust.region,

			s_city:   supp.city,
			s_nation: supp.nation,
			s_region: supp.region,

			p_mfgr:     par.mfgr,
			p_category: par.category,
			p_brand1:   par.brand1,

			order_year:    uint16(dat.year),
			order_month:   dat.month,
			order_weeknum: uint8(dat.weeknum),
		}
	}
	if err := scanner.Err(); err != nil {
		log.Println(errors.Wrap(err, "reading lineorder table"))
	}
}

func (m *Main) setupEdgeTables() (cust map[int]customer, par map[int]part, supp map[int]supplier, dat map[int]date, err error) {
	wg := sync.WaitGroup{}
	custF, err := os.Open(m.Dir + "/customer.tbl")
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "opening customer.tbl")
	}
	partF, err := os.Open(m.Dir + "/part.tbl")
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "opening part.tbl")
	}
	supplierF, err := os.Open(m.Dir + "/supplier.tbl")
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "opening supplier.tbl")
	}
	dateF, err := os.Open(m.Dir + "/date.tbl")
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "opening date.tbl")
	}
	wg.Add(4)
	go func() {
		defer wg.Done()
		cust = mapCustomer(custF, m.SFHint)
	}()
	go func() {
		defer wg.Done()
		par = mapPart(partF, m.SFHint)
	}()
	go func() {
		defer wg.Done()
		supp = mapSupplier(supplierF, m.SFHint)
	}()
	go func() {
		defer wg.Done()
		dat = mapDate(dateF, m.SFHint)
	}()
	wg.Wait()
	return cust, par, supp, dat, nil
}

var frames = []pdk.FrameSpec{
	// LO_
	pdk.NewRankedFrameSpec("lo_year", 10),
	pdk.NewRankedFrameSpec("lo_month", 12),
	pdk.NewRankedFrameSpec("lo_weeknum", 52),
	pdk.NewFieldFrameSpec("lo_quantity", 0, 63),
	pdk.NewFieldFrameSpec("lo_extendedprice", 0, 65535),
	pdk.NewFieldFrameSpec("lo_discount", 0, 15),
	pdk.NewFieldFrameSpec("lo_revenue", 0, 134217727),
	pdk.NewFieldFrameSpec("lo_supplycost", 0, 131071),
	pdk.NewFieldFrameSpec("lo_profit", 0, 134217727),
	pdk.NewFieldFrameSpec("lo_revenue_computed", 0, 65535),
	// pdk.NewFrameSpec("lo_tax"),
	// pdk.NewFrameSpec("lo_commityear"),
	// pdk.NewFrameSpec("lo_commitmonth"),
	// pdk.NewFrameSpec("lo_commitday"),
	// pdk.NewFrameSpec("lo_shipmode"),
	// pdk.NewFrameSpec("lo_orderpriority"),
	// pdk.NewFrameSpec("lo_shippriority"),

	// C_
	pdk.NewRankedFrameSpec("c_city", 500),
	pdk.NewRankedFrameSpec("c_nation", 30),
	pdk.NewRankedFrameSpec("c_region", 10),
	// pdk.NewFrameSpec("c_phone"),
	// pdk.NewFrameSpec("c_mktsegment"),
	// pdk.NewFrameSpec("c_name"),
	// pdk.NewFrameSpec("c_address"),

	// S_
	pdk.NewRankedFrameSpec("s_city", 500),
	pdk.NewRankedFrameSpec("s_nation", 30),
	pdk.NewRankedFrameSpec("s_region", 10),
	// pdk.NewFrameSpec("s_name"),
	// pdk.NewFrameSpec("s_address"),
	// pdk.NewFrameSpec("s_phone"),
	// pdk.NewFrameSpec("s_nation_prefix"),

	// P_
	pdk.NewRankedFrameSpec("p_mfgr", 20),
	pdk.NewRankedFrameSpec("p_category", 50),
	pdk.NewRankedFrameSpec("p_brand1", 2000),
	// pdk.NewFrameSpec("key"),
	// pdk.NewFrameSpec("name"),
	// pdk.NewFrameSpec("color"),
	// pdk.NewFrameSpec("type"),
	// pdk.NewFrameSpec("size"),
	// pdk.NewFrameSpec("container"),
}

type customer struct {
	city       []byte
	nation     []byte
	region     []byte
	mktsegment []byte
}

func mapCustomer(f io.Reader, sf int) map[int]customer {
	cmap := make(map[int]customer, 30000*sf)
	scanner := bufio.NewScanner(f)
	i := -1
	for scanner.Scan() {
		i++
		line := bytes.Split(scanner.Bytes(), []byte{'|'})
		key, err := strconv.Atoi(string(line[0]))
		if err != nil {
			log.Printf("Line %v of customer table: %v. Converting key to int: %v", i, line, err)
			continue
		}
		cmap[key] = customer{
			city:       line[3],
			nation:     line[4],
			region:     line[5],
			mktsegment: line[7],
		}
	}
	return cmap
}

type supplier struct {
	city   []byte
	nation []byte
	region []byte
}

func mapSupplier(f io.Reader, sf int) map[int]supplier {
	cmap := make(map[int]supplier, 2000*sf)
	scanner := bufio.NewScanner(f)
	i := -1
	for scanner.Scan() {
		i++
		line := bytes.Split(scanner.Bytes(), []byte{'|'})
		key, err := strconv.Atoi(string(line[0]))
		if err != nil {
			log.Printf("Line %v of supplier table: %v. Converting key to int: %v", i, line, err)
			continue
		}
		cmap[key] = supplier{
			city:   line[3],
			nation: line[4],
			region: line[5],
		}
	}
	return cmap
}

type part struct {
	mfgr     []byte
	category []byte
	brand1   []byte
}

func mapPart(f io.Reader, sf int) map[int]part {
	cmap := make(map[int]part, 2000*sf)
	scanner := bufio.NewScanner(f)
	i := -1
	for scanner.Scan() {
		i++
		line := bytes.Split(scanner.Bytes(), []byte{'|'})
		key, err := strconv.Atoi(string(line[0]))
		if err != nil {
			log.Printf("Line %v of part table: %v. Converting key to int: %v", i, line, err)
			continue
		}
		cmap[key] = part{
			mfgr:     line[2],
			category: line[3],
			brand1:   line[4],
		}
	}
	return cmap
}

type date struct {
	year    int
	month   []byte
	weeknum int
}

func (d date) String() string {
	return fmt.Sprintf("{year: %d, month: %s, weeknum: %d}", d.year, d.month, d.weeknum)
}

func mapDate(f io.Reader, sf int) map[int]date {
	cmap := make(map[int]date, 2000*sf)
	scanner := bufio.NewScanner(f)
	i := -1
	for scanner.Scan() {
		i++
		line := bytes.Split(scanner.Bytes(), []byte{'|'})
		key, err := strconv.Atoi(string(line[0]))
		if err != nil {
			log.Printf("Line %v of date table: %v. Converting key to int: %v", i, line, err)
			continue
		}
		year, err := strconv.Atoi(string(line[4]))
		if err != nil {
			log.Printf("Line %v of date table: %v. Converting year to int: %v", i, line, err)
			continue
		}
		weeknum, err := strconv.Atoi(string(line[11]))
		if err != nil {
			log.Printf("Line %v of date table: %v. Converting weeknum to int: %v", i, line, err)
			continue
		}
		month := make([]byte, len(line[3]))
		copy(month, line[3])
		d := date{
			year:    year,
			month:   month,
			weeknum: weeknum,
		}
		cmap[key] = d
	}
	return cmap
}
