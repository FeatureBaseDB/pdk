package ssb

import (
	"strings"
	"testing"
)

func TestMapCustomer(t *testing.T) {
	data := `
1|Customer#000000001|j5JsirBM9P|MOROCCO  2|MOROCCO|AFRICA|25-918-335-1736|BUILDING|
2|Customer#000000002|487LW1dovn6Q4dMVym|JORDAN   0|JORDAN|MIDDLE EAST|23-679-861-2259|AUTOMOBILE|
3|Customer#000000003|fkRGN8n|ARGENTINA0|ARGENTINA|AMERICA|11-383-516-1199|AUTOMOBILE|
4|Customer#000000004|4u58h f|EGYPT    9|EGYPT|MIDDLE EAST|14-843-787-7479|MACHINERY|
5|Customer#000000005|hwBtxkoBF qSW4KrI|CANADA   7|CANADA|AMERICA|13-151-690-3663|HOUSEHOLD|
6|Customer#000000006| g1s,pzDenUEBW3O,2 pxu|SAUDI ARA1|SAUDI ARABIA|MIDDLE EAST|30-696-997-4969|AUTOMOBILE|
7|Customer#000000007|8OkMVLQ1dK6Mbu6WG9|CHINA    1|CHINA|ASIA|28-990-965-2201|AUTOMOBILE|
8|Customer#000000008|j,pZ,Qp,qtFEo0r0c 92qo|PERU     3|PERU|AMERICA|27-498-742-3860|BUILDING|
9|Customer#000000009|vgIql8H6zoyuLMFN|INDIA    4|INDIA|ASIA|18-403-398-8662|FURNITURE|
10|Customer#000000010|Vf mQ6Ug9Ucf5OKGYq fs|ETHIOPIA 6|ETHIOPIA|AFRICA|15-852-489-8585|HOUSEHOLD|
`[1:]

	r := strings.NewReader(data)
	res := mapCustomer(r, 10)

	if len(res) != 10 {
		t.Fatalf("unexpected number of customers in result map: %v\n%v", len(res), res)
	}
	if string(res[8].mktsegment) != "BUILDING" {
		t.Fatalf("res8.mktsegment: %s doesn't match BUILDING", res[8].mktsegment)
	}
	if string(res[1].nation) != "MOROCCO" {
		t.Fatalf("res1.nation: %s doesn't match MOROCCO", res[1].nation)
	}
}

func TestMapSupplier(t *testing.T) {
	data := `
1|Supplier#000000001|sdrGnXCDRcfriBvY0KL,i|PERU     9|PERU|AMERICA|27-989-741-2988|
2|Supplier#000000002|TRMhVHz3XiFu|ETHIOPIA 7|ETHIOPIA|AFRICA|15-768-687-3665|
3|Supplier#000000003|BZ0kXcHUcHjx62L7CjZS|ARGENTINA2|ARGENTINA|AMERICA|11-719-748-3364|
4|Supplier#000000004|qGTQJXogS83a7MB|MOROCCO  7|MOROCCO|AFRICA|25-128-190-5944|
5|Supplier#000000005|lONEYAh9sF|IRAQ     6|IRAQ|MIDDLE EAST|21-750-942-6364|
6|Supplier#000000006|zaux5FT|KENYA    2|KENYA|AFRICA|24-114-968-4951|
7|Supplier#000000007| 0W7IPdkpWycU|UNITED KI6|UNITED KINGDOM|EUROPE|33-190-982-9759|
8|Supplier#000000008|S8AWPqjYlanEQlcDO2|PERU     7|PERU|AMERICA|27-147-574-9335|
9|Supplier#000000009|,gJ6K2MKveYxQT|IRAN     2|IRAN|MIDDLE EAST|20-338-906-3675|
10|Supplier#000000010|9QtKQKXK24f|UNITED ST0|UNITED STATES|AMERICA|34-741-346-9870|
`[1:]

	r := strings.NewReader(data)
	res := mapSupplier(r, 10)

	if len(res) != 10 {
		t.Fatalf("unexpected number of suppliers in result map: %v\n%v", len(res), res)
	}
	if string(res[8].city) != "PERU     7" {
		t.Fatalf("res8.city: %s doesn't match PERU     7", res[8].city)
	}
	if string(res[1].nation) != "PERU" {
		t.Fatalf("res1.nation: %s doesn't match PERU", res[1].nation)
	}
}

func TestMapPart(t *testing.T) {
	data := `
1|lace spring|MFGR#1|MFGR#11|MFGR#1121|goldenrod|PROMO BURNISHED COPPER|7|JUMBO PKG|
2|rosy metallic|MFGR#4|MFGR#43|MFGR#4318|blush|LARGE BRUSHED BRASS|1|LG CASE|
3|green antique|MFGR#3|MFGR#32|MFGR#3210|dark|STANDARD POLISHED BRASS|21|WRAP CASE|
4|metallic smoke|MFGR#1|MFGR#14|MFGR#1426|chocolate|SMALL PLATED BRASS|14|MED DRUM|
5|blush chiffon|MFGR#4|MFGR#45|MFGR#4510|forest|STANDARD POLISHED TIN|15|SM PKG|
6|ivory azure|MFGR#2|MFGR#23|MFGR#2325|white|PROMO PLATED STEEL|4|MED BAG|
7|blanched tan|MFGR#5|MFGR#51|MFGR#513|blue|SMALL PLATED COPPER|45|SM BAG|
8|khaki cream|MFGR#1|MFGR#13|MFGR#1328|ivory|PROMO BURNISHED TIN|41|LG DRUM|
9|rose moccasin|MFGR#4|MFGR#41|MFGR#4117|thistle|SMALL BURNISHED STEEL|12|WRAP CASE|
10|moccasin royal|MFGR#2|MFGR#21|MFGR#2128|floral|LARGE BURNISHED STEEL|44|LG CAN|
`[1:]

	r := strings.NewReader(data)
	res := mapPart(r, 10)

	if len(res) != 10 {
		t.Fatalf("unexpected number of parts in result map: %v\n%v", len(res), res)
	}
	if string(res[8].mfgr) != "MFGR#1" {
		t.Fatalf("res8.mfgr: %s doesn't match MFGR#1", res[8].mfgr)
	}
	if string(res[5].category) != "MFGR#45" {
		t.Fatalf("res5.category: %s doesn't match MFGR#45", res[5].category)
	}
	if string(res[1].brand1) != "MFGR#1121" {
		t.Fatalf("res1.brand1: %s doesn't match MFGR#1121", res[1].brand1)
	}
}

func TestMapDate(t *testing.T) {
	data := `
19920101|January 1, 1992|Thursday|January|1992|199201|Jan1992|5|1|1|1|1|Winter|0|1|1|1|
19920102|January 2, 1992|Friday|January|1992|199201|Jan1992|6|2|2|1|1|Winter|0|1|0|1|
19920103|January 3, 1992|Saturday|January|1992|199201|Jan1992|7|3|3|1|1|Winter|1|1|0|0|
19920104|January 4, 1992|Sunday|January|1992|199201|Jan1992|1|4|4|1|1|Winter|0|1|0|0|
19920105|January 5, 1992|Monday|January|1992|199201|Jan1992|2|5|5|1|1|Winter|0|1|0|1|
19920106|January 6, 1992|Tuesday|January|1992|199201|Jan1992|3|6|6|1|1|Winter|0|1|0|1|
19920107|January 7, 1992|Wednesday|January|1992|199201|Jan1992|4|7|7|1|2|Winter|0|1|0|1|
19920108|January 8, 1992|Thursday|January|1992|199201|Jan1992|5|8|8|1|2|Winter|0|1|0|1|
19920109|January 9, 1992|Friday|January|1992|199201|Jan1992|6|9|9|1|2|Winter|0|1|0|1|
19920110|January 10, 1992|Saturday|January|1992|199201|Jan1992|7|10|10|1|2|Winter|1|1|0|0|
`[1:]

	r := strings.NewReader(data)
	res := mapDate(r, 10)

	if len(res) != 10 {
		t.Fatalf("unexpected number of dates in result map: %v\n%v", len(res), res)
	}
	if res[19920108].year != 1992 {
		t.Fatalf("res8.year: %d doesn't match 1992: %#v", res[19920108].year, res)
	}
	if string(res[19920105].month) != "January" {
		t.Fatalf("res5.month: %s doesn't match January", res[19920105].month)
	}
	if res[19920101].weeknum != 1 {
		t.Fatalf("res1.weeknum: %d doesn't match 1", res[19920101].weeknum)
	}
}
