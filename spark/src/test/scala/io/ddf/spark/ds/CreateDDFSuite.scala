package io.ddf.spark.ds

import io.ddf.ds.UsernamePasswordCredential
import io.ddf.exception.UnauthenticatedDataSourceException
import io.ddf.spark.{ATestSuite, DelegatingDDFManager}
import org.scalatest.Matchers

import scala.collection.JavaConversions.mapAsJavaMap

class CreateDDFSuite extends ATestSuite with Matchers {
  val s3SourceUri = getConfig("s3.source.uri").getOrElse("s3://adatao-sample-data")
  val s3Manager = new DelegatingDDFManager(manager, s3SourceUri)

  val mysqlSourceUri = getConfig("mysql.source.uri").getOrElse("jdbc:mysql://ci.adatao.com:3306/testdata")
  val mysqlManager = new DelegatingDDFManager(manager, mysqlSourceUri)

  val hdfsManager = new DelegatingDDFManager(manager, "hdfs:;")

  val s3Credential = getCredentialFromConfig("s3.access", "s3.secret")
  val mysqlCredential = getCredentialFromConfig("mysql.user", "mysql.pwd")

  val crimes_schema =
    """ID int, CaseNumber string, Date string, Block string, IUCR string, PrimaryType string,
      | Description string, LocationDescription string, Arrest boolean, Domestic boolean, Beat int,
      | District int, Ward int, CommunityArea int, FBICode string, XCoordinate int,
      | YCoordinate int, Year int, UpdatedOn string, Latitude double, Longitude double,
      | Location string
    """.stripMargin.replace("\n", "")
  val result_schema =
    """ID int, FlagTsunami string, Year int, Month int, Day int, Hour int, Minute int,
      | Second double, FocalDepth int, EqPrimary double, EqMagMw double, EqMagMs double,
      | EqMagMb double, EqMagMl double, EqMagMfd double, EqMagUnk double, Intensity int,
      | Country string, State string, LocationName string, Latitude double, Longitude double,
      | RegionCode int, Death int, DeathDescription int, Injuries int, InjuriesDescription int
    """.stripMargin.replace("\n", "")
  val airline_schema =
    """year int, month int, dayofmonth int, dayofweek int, deptime int,crsdeptime int,
      | arrtime int, crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
      | actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, depdelay int,
      | origin string, dest string, distance int, taxiin int, taxiout int, cancelled int,
      | cancellationcode string, diverted string, carrierdelay int, weatherdelay int,
      | nasdelay int, securitydelay int, lateaircraftdelay int""".stripMargin.replace("\n", "")
  val sleep_schema = "StartTime int, SleepMinutes int, UID string, DeepSleepMinutes int, EndTime int"
  val mtcars_schema =
    """mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double,
      |vs int, am int, gear int, carb int""".stripMargin.replace("\n", "")

  val current_dir = sys.props.getOrElse("user.dir", "/")

  private def getCredentialFromConfig(usernameKey: String, passwordKey: String) = {
    val username = getConfig(usernameKey)
    val password = getConfig(passwordKey)
    if (username.isDefined && password.isDefined)
      Some(new UsernamePasswordCredential(username.get, password.get))
    else
      None
  }

  test("create ddf without credential") {
    val options = Map[AnyRef, AnyRef](
      "path" -> "/test/csv/noheader/",
      "format" -> "csv",
      "schema" -> crimes_schema
    )
    a[UnauthenticatedDataSourceException] should be thrownBy {
      s3Manager.createDDF(options)
    }
  }

  when(s3Credential.isDefined) test "create DDF from csv file on S3" in {
    val options = Map[AnyRef, AnyRef](
      "path" -> "/test/csv/noheader/",
      "format" -> "csv",
      "schema" -> crimes_schema,
      "credential" -> s3Credential.get
    )
    val ddf = s3Manager.createDDF(options)

    assert(ddf.getNumColumns == 22)
    assert(ddf.getNumRows == 100)
    assert(ddf.getColumnName(0) == "ID")
  }

  when(s3Credential.isDefined) test "create DDF from tsv file on S3" in {
    val options = Map[AnyRef, AnyRef](
      "path" -> "/test/tsv/noheader/",
      "format" -> "csv",
      "schema" -> result_schema,
      "delimiter" -> "\t",
      "credential" -> s3Credential.get
    )
    val ddf = s3Manager.createDDF(options)

    assert(ddf.getNumColumns == 27)
    assert(ddf.getNumRows == 73)
    assert(ddf.getColumnName(0) == "ID")
  }

  when(s3Credential.isDefined) test "create DDF from json file on S3" in {
    val options = Map[AnyRef, AnyRef](
      "path" -> "/test/json/noheader/",
      "format" -> "json",
      "schema" -> sleep_schema,
      "credential" -> s3Credential.get
    )
    val ddf = s3Manager.createDDF(options)

    assert(ddf.getNumColumns == 5)
    assert(ddf.getNumRows == 10000)
    assert(ddf.getColumnName(0) == "StartTime")
  }

  test("create DDF from parquet file local") {
    val options = Map[AnyRef, AnyRef](
      "path" -> s"$current_dir/resources/test/sleep.parquet",
      "format" -> "parquet"
    )
    val ddf = hdfsManager.createDDF(options)
    assert(ddf.getNumColumns == 5)
    assert(ddf.getNumRows == 10000)
    assert(ddf.getColumnName(0) == "deep_sleep_minutes")
  }

  when(mysqlCredential.isDefined) test "create DDF from MySQL" in {
    val options = Map[AnyRef, AnyRef](
      "table" -> "crimes_small_case_sensitive",
      "credential" -> mysqlCredential.get
    )
    val ddf = mysqlManager.createDDF(options)

    assert(ddf.getNumColumns == 22)
    assert(ddf.getNumRows == 40000)
    assert(ddf.getColumnName(0) == "ID")
  }

  test("create DDF from local file") {
    val options = Map[AnyRef, AnyRef](
      "path" -> s"$current_dir/resources/test/mtcars",
      "format" -> "csv",
      "delimiter" -> " ",
      "schema" -> mtcars_schema
    )
    val ddf = hdfsManager.createDDF(options)

    assert(ddf.getNumColumns == 11)
    assert(ddf.getNumRows == 32)
    assert(ddf.getColumnName(0) == "mpg")
    ddf.VIEWS.head(1).get(0) == "21.0"
  }

  ignore("create DDF from local hdfs, " +
    "ignore for now as spark-csv only support nullValue option in master branch") {
    val options = Map[AnyRef, AnyRef](
      "path" -> s"$current_dir/resources/test/airline",
      "format" -> "csv",
      "schema" -> airline_schema,
      "nullValue" -> "NA"
    )
    val ddf = hdfsManager.createDDF(options)

    assert(ddf.getNumColumns == 29)
    assert(ddf.getNumRows == 31)
    assert(ddf.getColumnName(0) == "year")
  }
}
