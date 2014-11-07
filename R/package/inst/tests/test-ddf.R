
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

test_that("basic statistics works", {
  ddf <- load.mtcars(dm)
  
  expect_is(ddf, "DDF")
  expect_identical(colnames(ddf), c("mpg","cyl","disp","hp","drat","wt","qesc","vs","am","gear","carb"))
  expect_equal(ncol(ddf), 11)
  expect_equal(nrow(ddf), 32)
  
  s <- summary(ddf)
  expect_true(abs(s[1,1]-20.091) < 0.001)
  
  ddf2 <- head(ddf)
  expect_is(ddf2, "data.frame")
  
  agg.res <- daggr(mpg ~ vs + carb, ddf, FUN=sum)
  expect_identical(agg.res$`sum(mpg)`, c(177.4, 129.4, 48.9, 94.6, 19.7, 120.9, 15.0, 37.0))
  
  agg.res <- daggr(mpg ~ vs + carb, ddf, FUN=mean)
  expect_identical(agg.res$`mean(mpg)`, c(25.34, 25.88, 16.30, 18.92, 19.70, 15.11, 15.00, 18.50))
  
  agg.res <- daggr(mpg ~ vs + carb, ddf, FUN=median)
  expect_identical(agg.res$`median(mpg)`, c(22.15, 23.60, 15.80, 17.10, 19.70, 14.30, 15.00, 17.80))
  
  agg.res <- daggr(cbind(mpg,hp) ~ vs + am, ddf, FUN=median)
  expect_identical(agg.res$`median(mpg)`, c(28.07, 20.30, 19.70, 15.20))
  
  agg.res <- daggr(ddf, agg.cols="sum(mpg), min(hp)", by="vs, am")
  expect_identical(agg.res$`sum(mpg)`, c(198.6, 145.2, 118.5, 180.6))
  
  agg.res <- daggr(ddf, agg.cols="sum(mpg), mean(carb))", by="vs, am")
  expect_identical(agg.res$`sum(mpg)`, c(198.6, 145.2, 118.5, 180.6))
  
  
  fn <- fivenum(ddf)
  expect_equivalent(fn[1,], c(10.400, 4.000, 71.100, 52.000,  2.760,  1.513, 14.500,  0.000,  0.000,  3.000))
  
  
  spl <- sample(ddf, 10L)
  expect_equivalent(spl[,1], c(15.2, 15.5, 33.9, 21, 21, 19.2, 18.7, 17.8, 30.4, 14.7))

  newddf <- ddf[,c("mpg","wt")]
  df.test <- as.data.frame(newddf)
  expect_equivalent(df.test[,1],c(21.0, 21.0, 22.8, 21.4, 18.7, 18.1, 14.3, 24.4, 22.8, 19.2, 17.8, 16.4, 17.3, 15.2, 10.4, 10.4, 14.7, 32.4, 30.4, 33.9, 21.5, 15.5, 15.2, 13.3, 19.2, 27.3, 26.0, 30.4, 15.8, 19.7, 15.0, 21.4))

})


test_that("subsetting works", {
  ddf <- load.mtcars(dm)
  
  # pass tests
  ddf2 <- ddf[,c("mpg")]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,"mpg"]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c("mpg", "hp")]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg", "hp"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c(1)]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,c(1,2)]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), c("mpg", "cyl"))
  expect_equal(nrow(ddf2), 32)
  
  ddf2 <- ddf[,]
  expect_is(ddf2, "DDF")
  expect_equivalent(colnames(ddf2), colnames(ddf))
  expect_equal(nrow(ddf2), 32)
  
  # fail tests
  expect_error(ddf2 <- ddf[,c("mp")])
  
  expect_error(ddf2 <- ddf[,c(-1)])
  
  expect_error(ddf2 <- ddf[,c(12)])
  
  expect_error(ddf2 <- ddf[,c(1.2)])
  
  expect_error(ddf2 <- ddf[,c("mpg", 4)])
})

shutdown(dm)