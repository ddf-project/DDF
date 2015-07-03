"""
Created on Jun 22, 2014

@author: nhanitvn
"""

import unittest
from ddf import DDFManager, DDF_HOME

class TestDDFManager(unittest.TestCase):

    def setUp(self):
        self.dm = DDFManager('spark')

    def tearDown(self):
        self.dm.shutdown()

    def testSql(self):
        self.dm.sql('set hive.metastore.warehouse.dir=/tmp')
        self.dm.sql('drop table if exists airline_na')
        self.dm.sql("""create table airline_na (Year int,Month int,DayofMonth int,
             DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,
             CRSArrTime int,UniqueCarrier string, FlightNum int,
             TailNum string, ActualElapsedTime int, CRSElapsedTime int,
             AirTime int, ArrDelay int, DepDelay int, Origin string,
             Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,
             CancellationCode string, Diverted string, CarrierDelay int,
             WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ','        
        """)
        self.dm.sql("load data local inpath '{}/resources/test/airlineWithNA.csv' "
                    "into table airline_na".format(DDF_HOME))
        
        ddf = self.dm.sql2ddf('select * from airline_na')
        self.assertEqual(ddf.rows, 31)
        self.assertEqual(ddf.cols, 29)

if __name__ == '__main__':
    unittest.main()
