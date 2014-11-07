
library(testthat)
library(ddf)
context("DDF")

load.mtcars <- function(dm) {
  write.table(mtcars, "/tmp/mtcars", row.names=F, col.names=F)
  sql(dm, 'set hive.metastore.warehouse.dir=/tmp/hive/warehouse')
  sql(dm, "drop table if exists mtcars")
  sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
  sql(dm, "LOAD DATA LOCAL INPATH '/tmp/mtcars' INTO TABLE mtcars")
  
  sql2ddf(dm, "select * from mtcars")
}

dm <- DDFManager()

test_that("clustering alg. works", {
  ddf <- load.mtcars(dm)  

  newddf <- ddf[,c("mpg","wt")]
  km <- ddfKMeans(newddf)
  clu <- predict(km,c(2,6))
  expect_equal(clu, 0)
})

shutdown(dm)