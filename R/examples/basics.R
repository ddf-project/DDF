
library(ddf)
dm <- DDFManager("spark")

write.table(mtcars, "/tmp/mtcars", row.names=F, col.names=F)

sql(dm, 'set hive.metastore.warehouse.dir=/tmp/hive/warehouse')
sql(dm, "drop table if exists mtcars")
sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
sql(dm, paste0("LOAD DATA LOCAL INPATH '/tmp/mtcars' INTO TABLE mtcars"))

ddf <- sql2ddf(dm, "select * from mtcars")

colnames(ddf)

ncol(ddf)
nrow(ddf)

summary(ddf)

head(ddf)

daggr(mpg ~ vs + carb, ddf, FUN=mean)
daggr(ddf, agg.cols="sum(mpg), min(hp)", by="vs, am")

fivenum(ddf)

sample(ddf, 10L)

shutdown(dm)
