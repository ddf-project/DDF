'''
Created on Jun 22, 2014

@author: nhanitvn
'''
import unittest
from ddf.DDFManager import DDFManager


class TestDDFManager(unittest.TestCase):


    def testInitAndShutdown(self):
        dm = None
        try:
            dm = DDFManager("spark")
        except Exception:
            self.fail("Cannot initialize a DDFManager for 'spark' engine")
        
        try:
            dm.shutdown()
        except Exception:
            self.fail("Cannot shutdown DDFManager object")
    
    def testSqls(self):
        dm = DDFManager("spark")
        dm.sql("set hive.metastore.warehouse.dir=/tmp")
        dm.sql("drop table if exists airline_na")
        dm.sql("""create table airline_na (Year int,Month int,DayofMonth int,
             DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,
             CRSArrTime int,UniqueCarrier string, FlightNum int,
             TailNum string, ActualElapsedTime int, CRSElapsedTime int,
             AirTime int, ArrDelay int, DepDelay int, Origin string,
             Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,
             CancellationCode string, Diverted string, CarrierDelay int,
             WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ','        
        """)
        # Current dir: os.getcwd() == tests
        dm.sql("load data local inpath '../../../../resources/test/airlineWithNA.csv' into table airline_na")
        
        ddf = dm.sql2ddf("select * from airline_na")
        self.assertEqual(ddf.getNumRows(), 31)
        self.assertEqual(ddf.getNumColumns(), 29)
        dm.shutdown()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
