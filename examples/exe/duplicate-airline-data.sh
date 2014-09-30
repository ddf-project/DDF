#!/bin/bash

/root/ephemeral-hdfs/bin/start-mapred.sh

function dup-hdfs-dir () {
	local fromdir=$1
	shift
	local todir=$1
	shift
	local marker=$1
	shift
	if [ -z $fromdir ] && [ $todir ] && [ $marker ]; then
		echo "empty argument" >&2
		exit 1
	fi
	
	# first copy
	~/ephemeral-hdfs/bin/hadoop distcp $fromdir $todir/ || exit 1

	# rename files $i to $i.dup.csv
	for i in $(~/ephemeral-hdfs/bin/hadoop fs -ls $todir/*.csv | awk '{print $8}'); do
		~/ephemeral-hdfs/bin/hadoop fs -mv $i $i.dup$marker.csv || exit 1
	done

	# second copy
	~/ephemeral-hdfs/bin/hadoop distcp -update $fromdir $todir/ || exit 1
}

function recreate-shark-table() {
	local table=$1
	shift
	local dir=$1
	shift
	if [ -z $table ] && [ -z dir ]; then
		echo "bad argument" >&2
		exit 1
	fi	
	/root/shark/bin/shark -e "drop table if exists $table; create external table $table (Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '"$dir"';"  || exit 1
}

function make-airline-2x () {
	dup-hdfs-dir /airline /airline-2x dup1 && recreate-shark-table airline_24g /airline-2x
}

function make-airline-4x () {
	dup-hdfs-dir /airline-2x /airline-4x dup2 && recreate-shark-table airline_48g /airline-4x
}

function make-airline-8x () {
	dup-hdfs-dir /airline-4x /airline-8x dup3 && recreate-shark-table airline_96g /airline-8x
}

# source bigr-env.sh so that hive conf (such as metastore conf) is in classpath
bigrenv=$(find . -regex '.*conf/bigr-env.sh$') 
if [ ! -f $bigrenv ]; then
  echo "fatal: $bigrenv file does not exist"
  exit 255
fi
source $bigrenv

