## Inspiration
- http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/
- https://github.com/toddwschneider/nyc-taxi-data
- http://tech.marksblogg.com/benchmarks.html

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
----------+-------
        1 |     3
        2 |     2
```sql
select passenger_count, avg(total_amount) from rides group by passenger_count;
```
 passenger_count | avg
-----------------+------
               1 |   14
               2 | 19.5
```sql
select passenger_count, extract(year from pickup_datetime), count(*) from rides group by 1, 2;
```
passenger_count | date_part | count
-----------------+-----------+-------
               2 |      2017 |     2
               1 |      2017 |     3
```sql
select passenger_count, extract(year from pickup_datetime), round(trip_distance), count(*) from rides group by 1, 2, 3 order by 2, 4 desc;
```
 passenger_count | date_part | round | count
-----------------+-----------+-------+-------
               2 |      2017 |     4 |     1
               2 |      2017 |     2 |     1
               1 |      2017 |     2 |     1
               1 |      2017 |     3 |     1
               1 |      2017 |     1 |     1
