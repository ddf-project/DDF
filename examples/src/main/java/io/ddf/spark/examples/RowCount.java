/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ddf.spark.examples;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

public class RowCount {

  public static void main(String[] args) throws DDFException {

    DDFManager manager = DDFManager.get("spark");
    manager.sql("drop table if exists airline", "SparkSQL");

    manager.sql("create table airline (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

    manager.sql("load data local inpath 'resources/test/airline.csv' into table airline", "SparkSQL");

    DDF ddf = manager.sql2ddf("SELECT * FROM AIRLINE", "SparkSQL");

    long nrow = ddf.getNumRows();
    int ncol = ddf.getNumColumns();

    System.out.println("Number of data row is " + nrow);
    System.out.println("Number of data columns is " + ncol);

  }
}
