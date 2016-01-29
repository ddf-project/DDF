from __future__ import unicode_literals
from ddf import DDFManager, DDF_HOME, ml


dm = DDFManager("flink")

dm.sql('DROP TABLE IF EXISTS mtcars', False)
dm.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, "
       "qesc double, vs int, am int, gear int, carb string)", False)

dm.sql("LOAD {}/resources/test/mtcars delimited by ' ' INTO mtcars".format(DDF_HOME), False)

dm.sql("select count(*) from mtcars", False)
ddf = dm.sql2ddf("select * from mtcars", False)

print('Columns: ' + ', '.join(ddf.colnames))

print('Number of columns: {}'.format(ddf.cols))
print('Number of rows: {}'.format(ddf.rows))

print(ddf.summary())

print(ddf.head(2))

print(ddf.aggregate(['sum(mpg)', 'min(hp)'], ['vs', 'am']))

print(ddf.five_nums())

print(ddf.sample(3))

# Kmeans
km = ml.kmeans(ddf)
clu = km.predict(range(0, ddf.ncol))
print clu

# Linear Regression
lr = ml.linear_regression_gd(ddf)
lr.summary()

# Logistic Regression
lr = ml.logistic_regression_gd(ddf)
lr.summary()

dm.shutdown()
