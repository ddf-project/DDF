#!/bin/bash

echo "++++ Starting Hadoop MapReduce job tracker for distcp ..."

/root/ephemeral-hdfs/bin/start-mapred.sh

echo "++++ Running hadoop distcp to copy airline dataset from spark6 cluster to ephemeral-hdfs ..."

/root/ephemeral-hdfs/bin/hadoop distcp hdfs://smaster.adatao.com:9000/airline/ / || exit 1

# source bigr-env.sh so that hive conf (such as metastore conf) is in classpath
bigrenv=$(dirname $0)/../conf/bigr-env.sh
if [ ! -f $bigrenv ]; then
  echo "fatal: $bigrenv file does not exist"
  exit 255
fi
source $bigrenv

echo "++++ Creating table 'airline' in Hive metastore ..."

# drop table is necessary as the old table had absolute path to hdfs including namenode ip which is no longer current

/root/shark/bin/shark -e "drop table if exists airline; create external table airline (Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '\/airline';" || exit 1
