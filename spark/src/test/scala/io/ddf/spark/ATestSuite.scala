// scalastyle:off
package io.ddf.spark


import io.ddf.DDFManager
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

/**
  * This makes a Logger LOG variable available to the test suite.
  * Also makes beforeEach/afterEach as well as beforeAll/afterAll behaviors available.
  */
@RunWith(classOf[JUnitRunner])
abstract class ATestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass())
  val manager = DDFManager.get(DDFManager.EngineType.SPARK).asInstanceOf[SparkDDFManager]

  def truncate(x: Double, n: Int) = {
    def p10(n: Int, pow: Long = 10): Long = if (n == 0) pow else p10(n - 1, pow * 10)
    if (n < 0) {
      val m = p10(-n).toDouble
      math.round(x / m) * m
    }
    else {
      val m = p10(n - 1).toDouble
      math.round(x * m) / m
    }
  }

  def createTableText8Sample () {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists text8sample", "SparkSQL")
    manager.sql("create table text8sample (v1 string)" +
       " row format delimited fields terminated by ','", "SparkSQL")
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/text8_small' " +
       "INTO TABLE text8sample", "SparkSQL")
  }
  
  /**
    * Get config value from system property or environment variable.
    *
    * @param key
    * @return
    */
  protected def getConfig(key: String): Option[String] = {
    val sysEnvName = key.toUpperCase().replace(".", "_")
    sys.props.get(key).orElse(sys.env.get(sysEnvName))
  }

  /**
    * This enable the "when <condition> test <name> in {func}" syntax
    * to test the function only when some condition is true.
    */
  def when(condition: Boolean): When = new When(condition)

  protected class When(condition: Boolean) {
    def test(testName: String): Test = new Test(condition, testName)

    protected class Test(condition: Boolean, testName: String) {
      def in(testFun: => Unit) = {
        if (condition)
          ATestSuite.this.test(testName) {
            testFun
          }
        else
          ignore(testName) {
            testFun
          }
      }
    }

  }

  def createTableMtcars() {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists mtcars", "SparkSQL")
    manager.sql("CREATE TABLE mtcars ("
      + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
      + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '", "SparkSQL")
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/mtcars' INTO TABLE mtcars", "SparkSQL")
  }

  def createTableCarOwner() {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists carowner", "SparkSQL")
    manager.sql(" create table carowner (name string, cyl int, disp double) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '", "SparkSQL")
    manager.sql("load data local inpath '${hiveconf:shark.test.data.path}/test/carowner' into table carowner", "SparkSQL")
  }

  def createTableFactor(): Unit = {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists factor", "SparkSQL")
    manager.sql("create table factor(name string, num double) row format delimited fields terminated by ','", "SparkSQL")
    manager.sql("load data local inpath '${hiveconf:shark.test.data.path}/test/factor' into table factor", "SparkSQL")
  }

  def createTableAdmission() = {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists admission", "SparkSQL")
    manager.sql("create table admission (v1 int, v2 int, v3 double, v4 int)" +
      " row format delimited fields terminated by ' '", "SparkSQL")
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/admission.csv' " +
      "INTO TABLE admission", "SparkSQL")
  }

  def createTableAirlineSmall() {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists airline", "SparkSQL")
    manager.sql("create table airline (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airline.csv' " +
      "INTO TABLE airline", "SparkSQL")
  }

  def createTableAirline() {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists airline", "SparkSQL")
    manager.sql("create table airline (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airlineBig.csv' " +
      "INTO TABLE airline", "SparkSQL")
  }

  // to test new data types
  def createTableAirline_ColTypes() {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists airline_type", "SparkSQL")
    manager.sql("create table airline_type (Year int,Month tinyint,DayofMonth smallint," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum bigint, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airline.csv' " +
      "INTO TABLE airline_type", "SparkSQL")
  }

  def createTableAirlineWithNA() {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists airlineWithNA", "SparkSQL")
    manager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airlineWithNA.csv' " +
      "INTO TABLE airlineWithNA", "SparkSQL")
  }

}

/**
  * This logs the begin/end of each test with timestamps and test #
  */
abstract class ATimedTestSuite extends ATestSuite {
  private lazy val testNameArray: Array[String] = testNames.toArray
  private var testNumber: Int = 0

  def getCurrentTestName = "Test #%d: %s".format(testNumber + 1, testNameArray(testNumber))

  override def beforeEach = {
    LOG.info("%s started".format(this.getCurrentTestName))
  }

  override def afterEach = {
    testNumber += 1
    LOG.info("%s ended".format(this.getCurrentTestName))
    super.afterEach
  }

  override def afterAll = {
    manager.shutdown()
  }
}
