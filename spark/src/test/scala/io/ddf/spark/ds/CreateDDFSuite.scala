package io.ddf.spark.ds

import io.ddf.ds.{User, UsernamePasswordCredential}
import io.ddf.spark.{ATestSuite, DelegatingDDFManager}

import scala.collection.JavaConversions.mapAsJavaMap

class CreateDDFSuite extends ATestSuite {

  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true

  val s3SourceUri = getConfig("s3.source.uri").getOrElse("s3://adatao-sample-data")
  val mysqlSourceUri = getConfig("mysql.source.uri").getOrElse("jdbc:mysql://ci.adatao.com:3306/testdata")
  val s3Credential = getCredentialFromConfig("s3.access", "s3.secret")
  val mysqlCredential = getCredentialFromConfig("mysql.user", "mysql.pwd")

  val crimes_schema =
    """ID int, CaseNumber string, Date string, Block string, IUCR string, PrimaryType string,
      | Description string, LocationDescription string, Arrest int, Domestic int, Beat int,
      | District int, Ward int, CommunityArea int, FBICode string, XCoordinate int,
      | YCoordinate int, Year int, UpdatedOn string, Latitude double, Longitude double,
      | Location string
    """.stripMargin.replaceAll("\n", "")
  val sleep_schema = "StartTime int, SleepMinutes int, UID string, DeepSleepMinutes int, EndTime int"

  private def getCredentialFromConfig(usernameKey: String, passwordKey: String) = {
    val username = getConfig(usernameKey)
    val password = getConfig(passwordKey)
    if (username.isDefined && password.isDefined)
      Some(new UsernamePasswordCredential(username.get, password.get))
    else
      None
  }

  override protected def beforeAll(): Unit = {
    val user = new User("foo")
    User.setCurrentUser(user)
    getCredentialFromConfig("s3.access", "s3.secret") map {
      user.addCredential(s3SourceUri, _)
    }
    getCredentialFromConfig("mysql.user", "mysql.pwd") map {
      user.addCredential(mysqlSourceUri, _)
    }
  }

  testIf(s3Credential.isDefined, "create DDF from csv file on S3") {
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

  testIf(s3Credential.isDefined, "create DDF from json file on S3") {
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

  testIf(mysqlCredential.isDefined, "create DDF from MySQL") {
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
