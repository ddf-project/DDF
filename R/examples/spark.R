# Data source setting up
library(RMySQL)
con <- dbConnect(MySQL(),
                 user = 'pauser',
                 password = 'papwd',
                 host = 'localhost',
                 dbname='test')
dbWriteTable(conn = con, name = 'mtcars', value = mtcars, row.names=F)

write.table(mtcars, "/tmp/mtcars", row.names=F, col.names=F)

# DDF part
library(ddf)
dm <- DDFManager("spark")
# First way to import data
sql(dm, 'set hive.metastore.warehouse.dir=/tmp/hive/warehouse', data.source="SparkSQL")
sql(dm, "drop table if exists mtcars", data.source="SparkSQL")
sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '", data.source="SparkSQL")
sql(dm, "LOAD DATA LOCAL INPATH '/tmp/mtcars' INTO TABLE mtcars", data.source="SparkSQL")

ddf <- sql2ddf(dm, "select * from mtcars", data.source="SparkSQL")

# Second way, from file directly
ddf <- load_file(dm, "/tmp/mtcars")

# Third way, from jdbc
uri <- "jdbc:mysql://localhost:3306/test"
username <- "pauser"
password <- "papwd"
table <- "mtcars"
ddf <- load_jdbc(dm, uri, username, password, table)


colnames(ddf)
ncol(ddf)
nrow(ddf)

summary(ddf)

head(ddf)

aggregate(mpg ~ vs + carb, ddf, FUN=mean)
aggregate(ddf, agg.cols="sum(mpg), min(hp)", by="vs, am")

fivenum(ddf)

sample(ddf, 10L)

# join
ddf2 <- merge(ddf, ddf, by="hp", type="inner")

# Kmeans
newddf <- ddf[,c("mpg","wt")] 
km <- ml.kmeans(newddf)
clu <- predict(km,c(2,6))

# Linear Regression
lr <- ml.linear.regression(ddf[,c("hp","mpg")])
summary(lr)

# Logistic Regression
lr2 <- ml.logistic.regression(ddf[,c("hp","mpg","vs")])
summary(lr2)

shutdown(dm)
