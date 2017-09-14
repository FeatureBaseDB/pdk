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
	Dir    string
	Hosts  []string
	Index  string
	SFHint int
}

func NewMain() *Main {
	return &Main{
		Index: "ssb",
	}
}

var frames = []pdk.FrameSpec{
	// LO_
	pdk.NewFrameSpec("lo_year"),
	pdk.NewFrameSpec("lo_month"),
	pdk.NewFrameSpec("lo_weeknum"),
	pdk.NewFrameSpec("lo_quantity"),
	pdk.NewFrameSpec("lo_extendedprice"),
	pdk.NewFrameSpec("lo_ordertotalprice"),
	pdk.NewFrameSpec("lo_discount"),
	pdk.NewFrameSpec("lo_revenue"),
	pdk.NewFrameSpec("lo_supplycost"),
	pdk.NewFrameSpec("lo_profit"),
	pdk.NewFrameSpec("lo_revenue_computed"),
	// pdk.NewFrameSpec("lo_tax"),
	// pdk.NewFrameSpec("lo_commityear"),
	// pdk.NewFrameSpec("lo_commitmonth"),
	// pdk.NewFrameSpec("lo_commitday"),
	// pdk.NewFrameSpec("lo_shipmode"),
	// pdk.NewFrameSpec("lo_orderpriority"),
	// pdk.NewFrameSpec("lo_shippriority"),

	// C_
	pdk.NewFrameSpec("c_city"),
	pdk.NewFrameSpec("c_nation"),
	pdk.NewFrameSpec("c_phone"),
	pdk.NewFrameSpec("c_mktsegment"),
	// pdk.NewFrameSpec("c_name"),
	// pdk.NewFrameSpec("c_address"),
	// pdk.NewFrameSpec("c_region"),

	// S_
	pdk.NewFrameSpec("s_city"),
	pdk.NewFrameSpec("s_nation"),
	pdk.NewFrameSpec("s_region"),
	// pdk.NewFrameSpec("s_name"),
	// pdk.NewFrameSpec("s_address"),
	// pdk.NewFrameSpec("s_phone"),
	// pdk.NewFrameSpec("s_nation_prefix"),

	// P_
	pdk.NewFrameSpec("mfgr"),
	pdk.NewFrameSpec("category"),
	pdk.NewFrameSpec("brand1"),
	// pdk.NewFrameSpec("key"),
	// pdk.NewFrameSpec("name"),
	// pdk.NewFrameSpec("color"),
	// pdk.NewFrameSpec("type"),
	// pdk.NewFrameSpec("size"),
	// pdk.NewFrameSpec("container"),
}

func (m *Main) Run() error {
	client, err := pdk.SetupPilosa(m.Hosts, m.Index, frames)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	// read all but the main lineorder table into memory
	cust, par, supp, dat, err := m.setupEdgeTables()
	if err != nil {
		return errors.Wrap(err, "setting up edge tables")
	}

	fmt.Println(cust, par, supp, dat)
	fmt.Println(client)
	return nil
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
		cmap[key] = date{
			year:    year,
			month:   line[3],
			weeknum: weeknum,
		}
	}
	return cmap
}
