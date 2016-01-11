from __future__ import unicode_literals
import unittest
from ddf import DDFManager, DDF_HOME


class BaseTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dm_spark = DDFManager('spark')
        cls.airlines = cls.loadAirlines()

    @classmethod
    def tearDownClass(cls):
        cls.dm_spark.shutdown()

    @classmethod
    def loadAirlines(cls):
        cls.dm_spark.sql('set hive.metastore.warehouse.dir=/tmp')
        cls.dm_spark.sql('drop table if exists airline_na')
        cls.dm_spark.sql("""create table airline_na (Year int,Month int,DayofMonth int,
             DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,
             CRSArrTime int,UniqueCarrier string, FlightNum int,
             TailNum string, ActualElapsedTime int, CRSElapsedTime int,
             AirTime int, ArrDelay int, DepDelay int, Origin string,
             Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,
             CancellationCode string, Diverted string, CarrierDelay int,
             WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        """)
        cls.dm_spark.sql("load data local inpath '{}/resources/test/airlineWithNA.csv' "
                         "into table airline_na".format(DDF_HOME))

        return cls.dm_spark.sql2ddf('select * from airline_na')
