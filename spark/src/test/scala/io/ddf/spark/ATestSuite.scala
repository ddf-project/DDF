// scalastyle:off
package io.ddf.spark


import org.scalatest.FunSuite
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.scalatest.BeforeAndAfterEach
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterAll
import io.ddf.DDFManager

/**
 * This makes a Logger LOG variable available to the test suite.
 * Also makes beforeEach/afterEach as well as beforeAll/afterAll behaviors available.
 */
@RunWith(classOf[JUnitRunner])
abstract class ATestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass())
  val manager = DDFManager.get("spark").asInstanceOf[SparkDDFManager]

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

  def createTableMtcars() {
    manager.sql("set shark.test.data.path=../resources")
    manager.sql("drop table if exists mtcars")
    manager.sql("CREATE TABLE mtcars ("
      + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
      + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/mtcars' INTO TABLE mtcars")
  }

  def createTableAdmission() = {
    manager.sql("set shark.test.data.path=../resources")
    manager.sql("drop table if exists admission")
    manager.sql("create table admission (v1 int, v2 int, v3 double, v4 int)" +
      " row format delimited fields terminated by ' '")
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/admission.csv' " +
      "INTO TABLE admission")
  }

  def createTableAirlineSmall() {
    manager.sql("set shark.test.data.path=../resources")
    manager.sql("drop table if exists airline")
    manager.sql("create table airline (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airline.csv' " +
      "INTO TABLE airline")
  }

  def createTableAirline() {
    manager.sql("set shark.test.data.path=../resources")
    manager.sql("drop table if exists airline")
    manager.sql("create table airline (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airlineBig.csv' " +
      "INTO TABLE airline")
  }

  def createTableAirlineWithNA() {
    manager.sql("set shark.test.data.path=../resources")
    manager.sql("drop table if exists airlineWithNA")
    manager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int," +
      "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," +
      "CRSArrTime int,UniqueCarrier string, FlightNum int, " +
      "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " +
      "AirTime int, ArrDelay int, DepDelay int, Origin string, " +
      "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " +
      "CancellationCode string, Diverted string, CarrierDelay int, " +
      "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/airlineWithNA.csv' " +
      "INTO TABLE airlineWithNA")
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
