package io.ddf.spark.ds

import io.ddf.ds.{User, UsernamePasswordCredential}
import io.ddf.spark.{ATestSuite, DelegatingDDFManager}

import scala.collection.JavaConversions.mapAsJavaMap

class CreateDDFSuite extends ATestSuite {
  val s3SourceUri = sys.env.getOrElse("S3_SOURCE_URI", "s3://adatao-sample-data")
  val mysqlSourceUri = "jdbc:mysql://ci.adatao.com:3306/testdata"
  var hasS3Credential = false
  var hasMySqlCredential = false

  val crimes_schema =
    """ID int, CaseNumber string, Date string, Block string, IUCR string, PrimaryType string,
      | Description string, LocationDescription string, Arrest int, Domestic int, Beat int,
      | District int, Ward int, CommunityArea int, FBICode string, XCoordinate int,
      | YCoordinate int, Year int, UpdatedOn string, Latitude double, Longitude double,
      | Location string
    """.stripMargin.replaceAll("\n", "")

  val sleep_schema = "StartTime int, SleepMinutes int, UID string, DeepSleepMinutes int, EndTime int"

  override protected def beforeAll(): Unit = {
    val user = new User("foo")
    User.setCurrentUser(user)

    val access_key = sys.env.get("S3_ACCESS")
    val secret_key = sys.env.get("S3_SECRET")
    if (access_key.isDefined && secret_key.isDefined) {
      val s3Cred = new UsernamePasswordCredential(access_key.get, secret_key.get)
      user.addCredential(s3SourceUri, s3Cred)
      hasS3Credential = true
    }

    val mysqlUsername = sys.env.get("MYSQL_USER")
    val mysqlPassword = sys.env.get("MYSQL_PWD")
    if (mysqlUsername.isDefined && mysqlPassword.isDefined) {
      val mysqlCred = new UsernamePasswordCredential(mysqlUsername.get, mysqlPassword.get)
      user.addCredential(mysqlSourceUri, mysqlCred)
      hasMySqlCredential = true
    }
  }

  test("create DDF from csv file on S3") {
    if (hasS3Credential) {
      val manager = new DelegatingDDFManager(this.manager, s3SourceUri)
      val options = Map[AnyRef, AnyRef](
        "path" -> "/test/csv/noheader/",
        "format" -> "csv",
        "schema" -> crimes_schema
      )
      val ddf = manager.createDDF(options)

      assert(ddf.getNumColumns == 22)
      assert(ddf.getNumRows == 100)
      assert(ddf.getColumnName(0) == "ID")
    }
  }

  test("create DDF from json file on S3") {
    val manager = new DelegatingDDFManager(this.manager, s3SourceUri)
    val options = Map[AnyRef, AnyRef](
      "path" -> "/test/json/noheader/",
      "format" -> "json",
      "schema" -> sleep_schema
    )
    val ddf = manager.createDDF(options)

    assert(ddf.getNumColumns == 5)
    assert(ddf.getNumRows == 10000)
    assert(ddf.getColumnName(0) == "StartTime")
  }

  test("create DDF from MySQL") {
    if (hasMySqlCredential) {
      val manager = new DelegatingDDFManager(this.manager, mysqlSourceUri)
      val options = Map[AnyRef, AnyRef](
        "table" -> "crimes_small_case_sensitive"
      )
      val ddf = manager.createDDF(options)

      assert(ddf.getNumColumns == 22)
      assert(ddf.getNumRows == 40000)
      assert(ddf.getColumnName(0) == "ID")
    }
  }
}
