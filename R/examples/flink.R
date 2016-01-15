library(RMySQL)
con <- dbConnect(MySQL(),
                 user = 'pauser',
                 password = 'papwd',
                 host = 'localhost',
                 dbname='test')
dbWriteTable(conn = con, name = 'mtcars', value = mtcars, row.names=F)

write.table(mtcars, "/tmp/mtcars", row.names=F, col.names=F)

library(ddf)
dm <- DDFManager("flink")

sql(dm, "CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string)", FALSE) 
sql(dm, "LOAD '/tmp/mtcars' delimited by ' ' INTO mtcars", FALSE) 

sql(dm, "CREATE TABLE flight (Year int,Month int,DayofMonth int, DayOfWeek int,
    DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, 
    FlightNum int, TailNum string, ActualElapsedTime int,CRSElapsedTime int, AirTime int, 
    ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, 
    TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, 
    WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int)", FALSE) 

sql(dm, "LOAD '../../resources/test/airlineWithNA.csv' delimited by ',' into flight", FALSE)

sql(dm, "select count(*) from mtcars", FALSE)
ddf <- sql2ddf(dm, "select * from mtcars", FALSE)

# ddf <- load_file(dm, "/tmp/mtcars")
# 
# uri <- "jdbc:mysql://localhost:3306/test"
# username <- "pauser"
# password <- "papwd"
# table <- "mtcars"
# ddf <- load_jdbc(dm, uri, username, password, table)

colnames(ddf)

ncol(ddf)
nrow(ddf)

sql(dm, "select count(*) from flight", FALSE)
jddf <- sql2ddf(dm, "select * from flight", FALSE)
sql(ddf, "select count(*) from @this")

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
