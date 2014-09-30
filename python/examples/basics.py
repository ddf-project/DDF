from ddf.DDFManager import DDFManager
dm = DDFManager("spark")

import os
DDF_HOME = os.getenv("DDF_HOME")

dm.sql("set hive.metastore.warehouse.dir=/tmp/hive/warehouse")
dm.sql("drop table if exists mtcars")
dm.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
dm.sql("LOAD DATA LOCAL INPATH '" + DDF_HOME  + "/resources/test/mtcars' INTO TABLE mtcars")

ddf = dm.sql2ddf("select * from mtcars")

ddf.getColumnNames()

ddf.getNumColumns()
ddf.getNumRows()

ddf.getSummary()

ddf.firstNRows(10)

ddf.aggregate("sum(mpg), min(hp)", "vs, am")

ddf.getFiveNumSummary()

ddf.sample(10)

dm.shutdown()

