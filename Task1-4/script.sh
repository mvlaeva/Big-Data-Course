#!/bin/bash
wget http://stat-computing.org/dataexpo/2009/carriers.csv 
hdfs dfs -mkdir /user/raj_ops/carriers/
hdfs dfs -put ./carriers.csv /user/raj_ops/carriers/

wget http://stat-computing.org/dataexpo/2009/airports.csv
hdfs dfs -mkdir /user/raj_ops/airports/
hdfs dfs -put ./airports.csv /user/raj_ops/airports/

wget http://stat-computing.org/dataexpo/2009/2007.csv.bz2
hdfs dfs -put ./2007.csv.bz2 /user/raj_ops/2007
hdfs dfs -mkdir /user/raj_ops/2007/
hdfs dfs -cat /user/raj_ops/2007/2007.csv.bz2 | bzip2 -d | hdfs dfs -put - /user/raj_ops/2007/2007.csv

beeline -u 'jdbc:hive2://localhost:10000 org.apache.hadoop.hive.jdbc.HiveDriver' 

CREATE EXTERNAL TABLE carriers(code STRING, description STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"" )
LOCATION '/user/raj_ops/carriers/'
TBLPROPERTIES ( 'skip.header.line.count'='1' )

CREATE EXTERNAL TABLE airports(iata VARCHAR(3), airport STRING, city STRING, state VARCHAR(2), country STRING, lat DOUBLE, long DOUBLE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"" )
LOCATION '/user/raj_ops/airports/'
TBLPROPERTIES ( 'skip.header.line.count'='1' )

CREATE EXTERNAL TABLE dveisedma(year INT, month INT, dayOfMonth INT, dayOfWeek INT, depTime INT, CRSDepTime VARCHAR(2), arrTime INT, uniqueCarrier VARCHAR(6), flightNum INT, tailNum INT, actualElapsedTime INT, CRSElapsedTime INT, airTime INT, arrDelay INT, depDelay INT, origin VARCHAR(3), dest VARCHAR(3), distance INT, taxiIn INT, taxiOut INT, cancelled BOOLEAN, cancellationCode INT, diverted BOOLEAN, carrierDelay INT, weatherDelay INT, NASDelay INT, securityDelay INT, lateAircraftDelay INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"" )
LOCATION '/user/raj_ops/2007/data'
TBLPROPERTIES ( 'skip.header.line.count'='2' )

select c.description as airline_carrier, COUNT(f.uniqueCarrier) as number_of_flights
from carriers as c 
join flights as f on f.unique_carrier = c.code where f.year = 2007
group by c.description

select SUM(flights) as total_flights
from (select COUNT(a.iata) as flights, a.airport from airports as a 
left join flights as f on f.origin = a.iata 
where a.city='New York' and f.month = 6 group by a.airport 
UNION 
select COUNT(a.iata) as flights, a.airport from airports as a 
left join flights as f on f.dest = a.iata 
where a.city='New York' and f.month = 6 group by a.airport) as x;
 
select SUM(served_flights) as sum_of_served_flights, airport from( 
select a.airport as airport, count(a.iata) as served_flights 
from dataset.airports as a 
join flights as f on f.dest = a.iata 
where f.month = 6 or f.month = 7 and f.year = 2007 and a.state = "USA"
group by airport 
UNION 
select a.airport as airport, count(a.iata) as served_flights 
from dataset.airports as a 
join flights as f on f.origin = a.iata 
where f.month = 6 or f.month = 7 and f.year = 2007 and a.state = "USA"
group by airport ) as x 
group by airport 
order by sum_of_served_flights desc 
limit 5 

select SUM(served_flights) as sum_of_served_flights, description from( 
select a.iata as iata, count(a.iata) as served_flights 
from dataset.airports as a 
join flights as f on f.dest = a.iata 
group by iata 
UNION 
select a.iata as iata, count(a.iata) as served_flights from dataset.airports as a
join flights as f on f.origin = a.iata 
group by iata ) as x join carriers c on c.code = x.iata 
group by description 
order by sum_of_served_flights desc 
limit 1


