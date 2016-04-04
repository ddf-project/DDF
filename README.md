DDF
===

Distributed DataFrame: Productivity = Power x Simplicity
For Big Data Scientists & Engineers

* [Visit DDF Website!](http://ddf.io)

* [Wiki](https://github.com/ddf-project/DDF/wiki)

* [Issues tracker](https://github.com/ddf-project/DDF/issues)

* [Questions/Comments/Feature Requests](https://groups.google.com/forum/#!forum/ddf-project)

# DDF - Distributed DataFrame 

DDF aims to make Big Data easy yet powerful, by bringing together
the best ideas from R Data Science, RDBMS/SQL, and Big Data distributed
processing.

It exposes high-level abstractions like RDBMS tables,
SQL queries, data cleansing and transformations, machine-learning
algorithms, even collaboration and authentication, etc., while
hiding all the complexities of parallel distributed processing
and data handling.

DDF is a general abstraction that can be implemented on multiple
execution and data engines. We are providing a native implementation
on Apache Spark, as it is today the most expressive in its DAG
parallelization and also most powerful in its in-memory distributed
dataset abstraction (RDD). With this release, DDF provides native
Spark support for R, Python, Java, Scala.

An aim of the DDF project is to shine a focus of Big Data conversations
on top-down, user-focussed simplicity and power, where "users" include
business analysts, data scientists, and high-level Big Data engineers.

---

### Directory Structure

| Directory | Description |
|-----------|-------------|
| bin | useful helper scripts |
| exe | DDF execution/launch scripts and executables |
| conf | DDF configuration files |
| R | DDF in R |
| python | DDF in python |
| core | DDF core API |
| spark | DDF Spark implementation |
| examples | DDF example API-user code |
| project | Scala build config files |

### Getting Started

First clone or fork a copy of DDF, e.g.:

```
$ git clone https://github.com/ddf-project/DDF 
```

Now set up the neccessary environment variables (MAVEN_OPTS, JAVA_TOOL_OPTIONS) for the build.

```
$ cd DDF
$ bin/set-env.sh
```
If you don't want to set up the environment variables every time,
please add the commands into ~/.bashrc or ~/.bash_profile.

If you don't have sbt and want to install, run the following command
```
$ bin/get-sbt.sh
```
This command will install a local sbt under bin/sbt

(Optional) The following regenerates Eclipse .project and .classpath files:

```
$ bin/make-eclipse-projects.sh
```

### Building `DDF_core` or `DDF_spark`
```
$ mvn clean install -DskipTests
```

Or if you want to just build separate modules, you can go into module
directories and build, for example,

```
$ (cd core ; mvn clean install -DskipTests)
$ (cd spark ; mvn clean install -DskipTests)
```

Or if you are more familar with the sbt tool,
```
sbt (bin/sbt) clean compile publishLocal
```
### Running tests
	
```
mvn test
<<<<<<< HEAD
```

Or if you want to run test for individual modules,
=======
```
Note: DataLoadTesting requires the following mysql configurations (If you don't want to configure mysql, please disable or ignore this test):
```
database: localhost:3306/test
username: pauser
password: papwd
create table commands:
mysql> create table mtcars(mpg double, cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int);
mysql> load data infile 'Path_To_DDF/DDF/resources/test/mtcars' into table mtcars fields terminated by ' ';
```

If you want to run test for individual modules,
>>>>>>> 6ab8241fdb1d1ab371de77dd68da96fa5d4a31e2
```
$ (cd core ; mvn test)
$ (cd spark ; mvn test)
```

Or if you are more familar with the sbt tool,
```
sbt (bin/sbt) test
<<<<<<< HEAD
``` 
=======
```
Run Examples
```
bin/run-example io.ddf.spark.examples.RowCount 
```
Interactive Programming with DDF Shell


Enter ddf java shell using:
```
bin/ddf-shell
```
Spark
```
// Start spark DDFManager
DDFManager sparkDDFManager = DDFManager.get("spark");
// Load table into spark
sparkDDFManager.sql("create table airline (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", Boolean.FALSE);
sparkDDFManager.sql("load data local inpath './resources/airlineWithNA.csv' into table airline", Boolean.FALSE);
// Create a ddf
DDF table = sparkDDFManager.sql2ddf("select * from airline", Boolean.FALSE);
// GetSummary
Summary[] summary = table.getSummary();
// Do transform
table = table.transform("dist= distance/2");
table.sql("select * from @this", "Get error");
```
JDBC and Flink are also supported under 
```
https://github.com/ddf-project/ddf-jdbc
https://github.com/ddf-project/ddf-flink
```

JDBC
```
// Start JDBC DDFManager
import io.ddf.datasource.JDBCDataSourceDescriptor;
// Please fill in the correct type, url, username and password here
// Current included engine types include "redshift" (for redshift), "postgres" (for postgres), "jdbc" (for mysql etc.). And configurations should be added in ddf-conf/ddf.ini.
// For exmaple: 
DDFManager jdbcDDFManager = DDFManager.get("redshift", new JDBCDataSourceDescriptor("jdbc:redshift://redshift.c3tst.us-east1.redshift.amazonaws.com:5439/mydb", "myusername", "mypwd", null));
// Create a ddf
DDF redshiftDDF = jdbcDDFManager.sql2ddf("select * from links", Boolean.FALSE);
// Copy ddf to spark
DDF copiedDDF = sparkDDFManager.copyFrom(redshiftDDF, "copiedDDF");
sparkDDFManager.sql("select * from copiedDDF");

```
Flink
```
// Start Flink DDFManager
DDFManager flinkDDFManager = DDFManager.get("flink");
// Create a ddf
flinkManager.sql("CREATE TABLE flight (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int, TailNum string, ActualElapsedTime int,CRSElapsedTime int, AirTime int, ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int)", Boolean.False);
flinkManager.sql("load './resources/airlineWithNA.csv' delimited by ',' into flight", Boolean.FALSE);
// run query
DDF flinkTable = flinkManager.sql2ddf("select * from flight", Boolean.FALSE);
System.out.println(flinkTable.getNumRows());
```
### Note
For JDBC, you should copy the corresponding jdbc driver under ./lib/


 
>>>>>>> 6ab8241fdb1d1ab371de77dd68da96fa5d4a31e2
