package io.ddf.test.it

import scala.collection.JavaConversions._

import io.ddf.{DDF, DDFManager}

trait SparkBaseSuite extends BaseSuite {

  override val engineName: String = "spark"
  override val manager: DDFManager = SparkBaseSuite.manager

  var mtcars: DDF = null
  var carOwners: DDF = null
  var airline: DDF = null
  var airlineWithoutDefault: DDF = null
  var airlineWithNA: DDF = null
  var yearNames: DDF = null
  var smiths: DDF = null

  private def loadTestDataFromFile(filePath: String,
                                   tableName: String,
                                   schema: String,
                                   delimiter: String): DDF = {
    val sqlResult = manager.sql(s"SHOW TABLES", engineName)
    val tables = sqlResult.getRows.map(row => row.split("\t")(0))

    if (!tables.contains(tableName)) {
      manager.sql(s"CREATE TABLE $tableName ($schema) " +
        s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '$delimiter'", engineName)
      manager.sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE $tableName", engineName)
    }
    manager.sql2ddf(s"SELECT * FROM $tableName", engineName)
  }


  override def loadMtCarsDDF(useCache: Boolean = false): DDF = {
    if (this.mtcars == null || !useCache) {
      this.mtcars = loadTestDataFromFile(getClass.getResource("/mtcars").getPath, "mtcars",
        config.getValue("schema", "mtcars"), " ")
    }
    this.mtcars
  }

  override def loadCarOwnersDDF(useCache: Boolean = false): DDF = {
    if (this.carOwners == null || !useCache) {
      this.carOwners = loadTestDataFromFile(getClass.getResource("/carowner.txt").getPath, "carowner",
        config.getValue("schema", "carowner"), " ")
    }
    this.carOwners
  }

  override def loadAirlineDDF(useCache: Boolean = false): DDF = {
    if (this.airline == null || !useCache) {
      this.airline = loadTestDataFromFile(getClass.getResource("/airline.csv").getPath, "airline",
        config.getValue("schema", "airline"), ",")
    }
    this.airline
  }

  override def loadAirlineDDFWithoutDefault(useCache: Boolean = false): DDF = {
    if (this.airlineWithoutDefault == null || !useCache) {
      this.airlineWithoutDefault = loadTestDataFromFile(getClass.getResource("/airline.csv").getPath,
        "airlineWithoutDefault",
        config.getValue("schema", "airline"), ",")
    }
    this.airlineWithoutDefault
  }

  override def loadAirlineDDFWithNA(useCache: Boolean = false): DDF = {
    if (this.airlineWithNA == null || !useCache) {
      this.airlineWithNA = loadTestDataFromFile(getClass.getResource("/airlineWithNA.csv").getPath, "airlineWithNA",
        config.getValue("schema", "airline"), ",")
    }
    this.airlineWithNA
  }

  override def loadYearNamesDDF(useCache: Boolean = false): DDF = {
    if (this.yearNames == null || !useCache) {
      this.yearNames = loadTestDataFromFile(getClass.getResource("/year_names.csv").getPath, "year_names",
        config.getValue("schema", "year-names"), ",")
    }
    this.yearNames
  }

  override def loadSmithsDDF(useCache: Boolean = false): DDF = {
    if (this.smiths == null || !useCache) {
      this.smiths = loadTestDataFromFile(getClass.getResource("/smiths").getPath, "smiths",
        config.getValue("schema", "smiths"), ",")
    }
    this.smiths
  }
}

object SparkBaseSuite {
  val engineName: String = "spark"
  val manager = DDFManager.get(DDFManager.EngineType.fromString(engineName))
}
