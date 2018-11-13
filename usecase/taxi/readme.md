## Inspiration
- http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/
- https://github.com/toddwschneider/nyc-taxi-data
- http://tech.marksblogg.com/benchmarks.html

## Data
```
$ wc *
        7625        22870      1147696 green_tripdata_2013-08.csv
       49649       148942      7685009 green_tripdata_2013-09.csv
      170010       510025     26375774 green_tripdata_2013-10.csv
      380976      1142923     59160743 green_tripdata_2013-11.csv
      602561      1807678     93586486 green_tripdata_2013-12.csv
      803855      2411557    124843531 green_tripdata_2014-01.csv
     1005245      3015727    156606941 green_tripdata_2014-02.csv
     1293474      3880414    202092093 green_tripdata_2014-03.csv
     1309158      3927466    204863497 green_tripdata_2014-04.csv
     1421506      4264510    222722613 green_tripdata_2014-05.csv
     1337762      4013278    209637247 green_tripdata_2014-06.csv
     1273976      3821920    199578900 green_tripdata_2014-07.csv
     1344944      4034824    210824549 green_tripdata_2014-08.csv
     1361896      4085680    213461849 green_tripdata_2014-09.csv
     1491269      4473799    233727643 green_tripdata_2014-10.csv
     1548162      4644478    242624000 green_tripdata_2014-11.csv
     1645790      4937362    257796237 green_tripdata_2014-12.csv
     1508502      4525504    243307680 green_tripdata_2015-01.csv
     1574831      4724491    254498963 green_tripdata_2015-02.csv
     1722575      5167723    278503144 green_tripdata_2015-03.csv
     1664395      4993183    269283302 green_tripdata_2015-04.csv
     1786849      5360545    289318204 green_tripdata_2015-05.csv
     1638869      4916605    265179055 green_tripdata_2015-06.csv
     1541672      4625014    246400857 green_tripdata_2015-07.csv
     1532344      4597030    245027110 green_tripdata_2015-08.csv
     1494927      4484779    239035648 green_tripdata_2015-09.csv
     1630537      4891609    260729773 green_tripdata_2015-10.csv
     1529985      4589953    244623294 green_tripdata_2015-11.csv
     1608291      4824871    257080956 green_tripdata_2015-12.csv
     1445287      4335856    230986242 green_tripdata_2016-01.csv
     1510724      4532167    241410651 green_tripdata_2016-02.csv
     1576395      4729180    251960424 green_tripdata_2016-03.csv
     1543927      4631776    246944499 green_tripdata_2016-04.csv
     1536981      4610938    245883437 green_tripdata_2016-05.csv
     1404728      4214179    224653682 green_tripdata_2016-06.csv
    14092415     42317358   2538104764 yellow_tripdata_2009-01.csv
    13380124     40182391   2415447672 yellow_tripdata_2009-02.csv
    14387373     43208432   2599574051 yellow_tripdata_2009-03.csv
    14294785     42930650   2583712231 yellow_tripdata_2009-04.csv
    14796315     44435437   2675188897 yellow_tripdata_2009-05.csv
    14184251     42596020   2571138287 yellow_tripdata_2009-06.csv
    13626105     40919547   2470272430 yellow_tripdata_2009-07.csv
    13686522     41100712   2480817920 yellow_tripdata_2009-08.csv
    13984889     41996037   2534793683 yellow_tripdata_2009-09.csv
    15604553     46858193   2831973622 yellow_tripdata_2009-10.csv
    14275341     42865079   2625071434 yellow_tripdata_2009-11.csv
    14583406     43787515   2682015370 yellow_tripdata_2009-12.csv
    14863780     44626593   2728058790 yellow_tripdata_2010-01.csv
    11145411     33476752   2047905308 yellow_tripdata_2010-02.csv
    12884364     38688461   2366707460 yellow_tripdata_2010-03.csv
    15144992     45467596   2777484798 yellow_tripdata_2010-04.csv
    15481353     46478458   2839839141 yellow_tripdata_2010-05.csv
    14825130     44506476   2720122667 yellow_tripdata_2010-06.csv
    14656521     44001311   2688620278 yellow_tripdata_2010-07.csv
    12528179     37584532   2295956258 yellow_tripdata_2010-08.csv
    15540211     46620628   2854569014 yellow_tripdata_2010-09.csv
    14199609     42598822   2608374430 yellow_tripdata_2010-10.csv
    13912312     41736931   2553334274 yellow_tripdata_2010-11.csv
    13819324     41457967   2536431376 yellow_tripdata_2010-12.csv
    13464998     40394989   2474554026 yellow_tripdata_2011-01.csv
    14202802     42608401   2604373779 yellow_tripdata_2011-02.csv
    16066352     48199051   2948633344 yellow_tripdata_2011-03.csv
    14718975     44156920   2703310900 yellow_tripdata_2011-04.csv
    15554870     46664605   2860029176 yellow_tripdata_2011-05.csv
    15097863     45293584   2776252237 yellow_tripdata_2011-06.csv
    14742563     44227684   2708392003 yellow_tripdata_2011-07.csv
    13262443     39787324   2432989761 yellow_tripdata_2011-08.csv
    14626750     43880245   2682104748 yellow_tripdata_2011-09.csv
    15707758     47123269   2884780798 yellow_tripdata_2011-10.csv
    14525864     43577587   2668405528 yellow_tripdata_2011-11.csv
    14925985     44777950   2740546498 yellow_tripdata_2011-12.csv
    14969134     44907397   2759167113 yellow_tripdata_2012-01.csv
    14983523     44950564   2778006393 yellow_tripdata_2012-02.csv
    16146925     48440770   2994922424 yellow_tripdata_2012-03.csv
    15477916     46433743   2869579142 yellow_tripdata_2012-04.csv
    15567527     46702576   2885513918 yellow_tripdata_2012-05.csv
    15096470     45289405   2799672092 yellow_tripdata_2012-06.csv
    14379309     43137922   2666615247 yellow_tripdata_2012-07.csv
    14381754     43145257   2665989916 yellow_tripdata_2012-08.csv
    14546856     43640563   2451418620 yellow_tripdata_2012-09.csv
    14522317     43566946   2427037091 yellow_tripdata_2012-10.csv
    13776032     41328091   2299132130 yellow_tripdata_2012-11.csv
    14696585     44089750   2454427349 yellow_tripdata_2012-12.csv
    14776617     44329846   2472351469 yellow_tripdata_2013-01.csv
    13990178     41970529   2344381323 yellow_tripdata_2013-02.csv
    15749230     47247685   2639810975 yellow_tripdata_2013-03.csv
    15100470     45301405   2533430839 yellow_tripdata_2013-04.csv
    15285051     45855148   2565221965 yellow_tripdata_2013-05.csv
    14385458     43156369   2417268120 yellow_tripdata_2013-06.csv
    13823842     41471521   2322372042 yellow_tripdata_2013-07.csv
    12597111     37791328   2103004295 yellow_tripdata_2013-08.csv
    14107695     42323080   2380395466 yellow_tripdata_2013-09.csv
    15004558     45013669   2532937359 yellow_tripdata_2013-10.csv
    14388453     43165354   2420444667 yellow_tripdata_2013-11.csv
    13971120     41913355   2350229417 yellow_tripdata_2013-12.csv
    13782494     41347494   2324817062 yellow_tripdata_2014-01.csv
    13063793     39191391   2205460986 yellow_tripdata_2014-02.csv
    15428129     46284399   2604044841 yellow_tripdata_2014-03.csv
    14618761     43856295   2463299452 yellow_tripdata_2014-04.csv
    14774043     44322141   2488952100 yellow_tripdata_2014-05.csv
    13813031     41439105   2326220589 yellow_tripdata_2014-06.csv
    13106367     39319113   2204602640 yellow_tripdata_2014-07.csv
    12688879     38066649   2133936569 yellow_tripdata_2014-08.csv
    13374018     40122066   2260069774 yellow_tripdata_2014-09.csv
    14232489     42697479   2409216587 yellow_tripdata_2014-10.csv
    13218218     39654666   2235168890 yellow_tripdata_2014-11.csv
    13014163     39042501   2198671160 yellow_tripdata_2014-12.csv
    12748987     38246959   1985964692 yellow_tripdata_2015-01.csv
    12450522     37351564   1945357622 yellow_tripdata_2015-02.csv
    13351610     40054828   2087971794 yellow_tripdata_2015-03.csv
    13071790     39215368   2046225765 yellow_tripdata_2015-04.csv
    13158263     39474787   2061869121 yellow_tripdata_2015-05.csv
    12324936     36974806   1932049357 yellow_tripdata_2015-06.csv
    11562784     34688350   1812530041 yellow_tripdata_2015-07.csv
    11130305     33390913   1744852237 yellow_tripdata_2015-08.csv
    11225064     33675190   1760412710 yellow_tripdata_2015-09.csv
    12315489     36946465   1931460927 yellow_tripdata_2015-10.csv
    11312677     33938029   1773468989 yellow_tripdata_2015-11.csv
    11460574     34381720   1796283025 yellow_tripdata_2015-12.csv
    10906859     32720575   1708674492 yellow_tripdata_2016-01.csv
    11382050     34146148   1783554554 yellow_tripdata_2016-02.csv
    12210953     36632857   1914669757 yellow_tripdata_2016-03.csv
    11934339     35803015   1872015980 yellow_tripdata_2016-04.csv
    11836854     35510560   1858526595 yellow_tripdata_2016-05.csv
    11135471     33406411   1748784180 yellow_tripdata_2016-06.csv
  1294452208   3884106480 224463910542 total
```
Total content length according to the sum of the content length of a HEAD request on each S3 object:
224463910542

total lines is 1294452208 - each file has a header and there are 125 files, so
 naively, there are 1,294,452,083 records.

actual (data2): 1266087512 + 28358395(skipped) = 1,294,445,907
                
ubuntu@ip-10-0-1-187:~$ curl http://10.0.1.152:10101/index/taxi/query -d"TopN(pickup_year)" | jq ".results[0] | map(.count) | add"
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   272  100   255  100    17   9364    624 --:--:-- --:--:-- --:--:--  9444
1266153241
ubuntu@ip-10-0-1-187:~$ curl http://10.0.1.152:10101/index/taxi/query -d"TopN(drop_year)" | jq ".results[0] | map(.count) | add"
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   270  100   255  100    15  12938    761 --:--:-- --:--:-- --:--:-- 13421
1266153241
ubuntu@ip-10-0-1-187:~$ curl http://10.0.1.152:10101/index/taxi/query -d"TopN(passenger_count)" | jq ".results[0] | map(.count) | add"
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   285  100   264  100    21   5450    433 --:--:-- --:--:-- --:--:--  5500
1266446584




## Pilosa queries

fields:
- cabType
- dist_miles
- dropDay
- dropGridID
- dropMonth
- dropTime
- duration_minutes
- pickupDay
- pickupGridID
- pickupMonth
- pickupTime
- speed_mph

also need passenger_count


1. count per cab_type
tick ; q=""; for i in {0..10} ; do q="${q}Count(Row(id=$i, field=cabType))" ; done ; curl localhost:15000/query?db=taxi -d "$q" ; tock


2. avg(total_amount) per passenger_count
  a. need a Sum() that works on attributes
  b. use binary representation of cents to get a Sum?

3. count per (passenger_count, year)
loop over x, y: count(intersect(x, y))

4. count per (passenger_count, year, round(trip_distance)) order by (year, count)
loop over x, y, z...
then do the ordering externally


## Mark queries
Mark uses four queries, I'm not sure if there is a place where he lays out exactly what they are.

### bigquery

```sql
bq query "SELECT cab_type,
                 count(*)
          FROM [taxis-1273:trips.taxi_trips]
          GROUP BY cab_type;"
```

```sql
bq query "SELECT passenger_count,
                 avg(total_amount)
          FROM [taxis-1273:trips.taxi_trips]
          GROUP BY passenger_count;"
```

```sql
bq query "SELECT passenger_count,
                 year(pickup_datetime),
                 count(*)
          FROM [taxis-1273:trips.taxi_trips]
          GROUP BY 1, 2;"
```

```sql
bq query "SELECT passenger_count,
                 year(pickup_datetime),
                 round(trip_distance),
                 count(*)
          FROM [taxis-1273:trips.taxi_trips]
          GROUP BY 1, 2, 3
          ORDER BY 2, 4 desc;"
```


### elasticsearch
```sql
SELECT cab_type,
       count(*)
FROM trips
GROUP BY cab_type
```

```sql
SELECT passenger_count,
       avg(total_amount)
FROM trips
GROUP BY passenger_count
```

```sql
SELECT passenger_count,
       count(*) trips
FROM trips
GROUP BY passenger_count,
         date_histogram(field='pickup_datetime',
                              'interval'='year',
                              'alias'='year')
```

## postgres sanity check

### setup
```sql
create database taxi;

\connect taxi;

create table rides (id int unique, cab_type int, passenger_count int, total_amount float, pickup_datetime timestamp, trip_distance float);

insert into rides (id, cab_type, passenger_count, total_amount, pickup_datetime, trip_distance) values (0, 1, 1, 20.00, '2017-02-10 06:00:00', 2);
insert into rides (id, cab_type, passenger_count, total_amount, pickup_datetime, trip_distance) values (1, 1, 1, 10.00, '2017-02-10 07:00:00', 3);
insert into rides (id, cab_type, passenger_count, total_amount, pickup_datetime, trip_distance) values (2, 1, 2, 15.00, '2017-02-10 08:00:00', 2.5);
insert into rides (id, cab_type, passenger_count, total_amount, pickup_datetime, trip_distance) values (3, 2, 1, 12.00, '2017-02-10 09:00:00', 1);
insert into rides (id, cab_type, passenger_count, total_amount, pickup_datetime, trip_distance) values (4, 2, 2, 24.00, '2017-02-10 10:00:00', 4);
```

### queries
```sql
select cab_type, count(*) from rides group by cab_type;
```
 cab_type | count
----------|-------
        1 |     3
        2 |     2
```sql
select passenger_count, avg(total_amount) from rides group by passenger_count;
```
 passenger_count | avg
-----------------|------
               1 |   14
               2 | 19.5
```sql
select passenger_count, extract(year from pickup_datetime), count(*) from rides group by 1, 2;
```
passenger_count | date_part | count
-----------------|-----------|-------
               2 |      2017 |     2
               1 |      2017 |     3
```sql
select passenger_count, extract(year from pickup_datetime), round(trip_distance), count(*) from rides group by 1, 2, 3 order by 2, 4 desc;
```
 passenger_count | date_part | round | count
-----------------|-----------|-------|-------
               2 |      2017 |     4 |     1
               2 |      2017 |     2 |     1
               1 |      2017 |     2 |     1
               1 |      2017 |     3 |     1
               1 |      2017 |     1 |     1
