library(testthat)
library(ddf)
context("DDFManager")

test_that("sql2ddf works", {
  write.table(mtcars, "/tmp/mtcars", row.names=F, col.names=F)
  dm <- DDFManager()
  sql(dm, 'set hive.metastore.warehouse.dir=/tmp/hive/warehouse')
  sql(dm, "drop table if exists mtcars")
  sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  sql(dm, "LOAD DATA LOCAL INPATH '/tmp/mtcars' INTO TABLE mtcars")
  ddf <- sql2ddf(dm, "select * from mtcars")
  expect_is(ddf, "DDF")
})