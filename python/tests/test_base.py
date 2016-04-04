from __future__ import unicode_literals
import unittest
from ddf import DDFManager, DDF_HOME


class BaseTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dm_spark = DDFManager('spark')
        cls.airlines = cls.loadAirlines(cls.dm_spark)
        cls.mtcars = cls.loadMtCars(cls.dm_spark)

    @classmethod
    def tearDownClass(cls):
        cls.dm_spark.shutdown()

    @classmethod
    def loadAirlines(cls, dm):
        table_name = 'airlines_na_pyddf_unittest'
        if table_name not in [x.split('\t')[0] for x in dm.sql('show tables')]:
            dm.sql('set hive.metastore.warehouse.dir=/tmp', False)
            dm.sql('drop table if exists {}'.format(table_name), False)
            dm.sql("""create table {} (Year int,Month int,DayofMonth int,
                 DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,
                 CRSArrTime int,UniqueCarrier string, FlightNum int,
                 TailNum string, ActualElapsedTime int, CRSElapsedTime int,
                 AirTime int, ArrDelay int, DepDelay int, Origin string,
                 Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,
                 CancellationCode string, Diverted string, CarrierDelay int,
                 WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            """.format(table_name), False)
            dm.sql("load data local inpath '{}/resources/test/airlineWithNA.csv' "
                   "into table {}".format(DDF_HOME, table_name), False)

        return dm.sql2ddf('select * from {}'.format(table_name), False)

    @classmethod
    def loadMtCars(cls, dm):
        table_name = 'mtcars_pyddf_unittest'
        if table_name not in [x.split('\t')[0] for x in dm.sql('show tables')]:
            dm.sql('set shark.test.data.path=resources', False)
            # session.sql('set hive.metastore.warehouse.dir=/tmp')
            dm.sql('drop table if exists {}'.format(table_name), False)
            dm.sql("CREATE TABLE {} (mpg double, cyl int, disp double, "
                   "hp int, drat double, wt double, "
                   "qesc double, vs int, am int, gear int, carb int)"
                   " ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '".format(table_name), False)
            dm.sql("LOAD DATA LOCAL INPATH '{}/resources/test/mtcars' "
                        "INTO TABLE {}".format(DDF_HOME, table_name), False)
        return dm.sql2ddf('select * from {}'.format(table_name), False)
