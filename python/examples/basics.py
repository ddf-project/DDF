from __future__ import unicode_literals
from ddf import DDFManager, DDF_HOME

dm = DDFManager('spark')

dm.sql('set hive.metastore.warehouse.dir=/tmp/hive/warehouse')
dm.sql('drop table if exists mtcars')
dm.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double,"
       " qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
dm.sql("LOAD DATA LOCAL INPATH '" + DDF_HOME + "/resources/test/mtcars' INTO TABLE mtcars")

ddf = dm.sql2ddf('select * from mtcars')

print('Columns: ' + ', '.join(ddf.colnames))

print('Number of columns: {}'.format(ddf.cols))
print('Number of rows: {}'.format(ddf.rows))

print(ddf.summary())

print(ddf.head(2))

print(ddf.aggregate(['sum(mpg)', 'min(hp)'], ['vs', 'am']))

print(ddf.five_nums())

print(ddf.sample(3))

dm.shutdown()

