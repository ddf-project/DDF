library(testthat)
library(ddf)
context("dplyr interface")

import.mtcars <- function(dm) {
  write.table(mtcars, "/tmp/mtcars", row.names=F, col.names=F)
  sql(dm, 'set hive.metastore.warehouse.dir=/tmp/hive/warehouse')
  sql(dm, "drop table if exists mtcars")
  sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  sql(dm, "LOAD DATA LOCAL INPATH '/tmp/mtcars' INTO TABLE mtcars")
}

test_that("basic statistics works", {
  ddflyr <- src_DDF()
  import.mtcars(ddflyr$con)
  db_list_tables(ddflyr$con)
  db_has_table(ddflyr$con, "mtcars")
  
  # this does not work with error
  # no applicable method for 'db_list_tables' applied to an object of class "DDFManager"
  # though the db_list_tables(ddflyr$con) work!!!
  ddf <- tbl(ddflyr, "mtcars")

})


