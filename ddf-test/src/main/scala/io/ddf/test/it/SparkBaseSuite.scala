package io.ddf.test.it

import io.ddf.datasource.{DataSourceDescriptor, JDBCDataSourceDescriptor, JDBCDataSourceCredentials, DataSourceURI}
import io.ddf.misc.Config
import io.ddf.{DDFManager, DDF}

trait SparkBaseSuite extends BaseSuite {

  override val engineName: String = "spark"
  override val manager: DDFManager = DDFManager.get(DDFManager.EngineType.fromString(engineName))


  private def loadTestDataFromFile(filePath: String,
                                   tableName: String,
                                   schema: String,
                                   delimiter: String): DDF = {

    manager.sql(s"DROP TABLE IF EXISTS $tableName", engineName)
    manager.sql(s"CREATE TABLE $tableName ($schema) " +
      s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '$delimiter'", engineName)
    manager.sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE $tableName", engineName)
    manager.sql2ddf(s"SELECT * FROM $tableName", engineName)
  }


  override def loadMtCarsDDF(): DDF = {
    loadTestDataFromFile(getClass.getResource("/mtcars").getPath, "mtcars",
      config.getValue("schema", "mtcars"), " ")
  }

  override def loadCarOwnersDDF(): DDF = {
    loadTestDataFromFile(getClass.getResource("/carowner.txt").getPath, "carowner",
      config.getValue("schema", "carowner"), " ")
  }

  override def loadAirlineDDF(): DDF = {
    loadTestDataFromFile(getClass.getResource("/airline.csv").getPath, "airline",
      config.getValue("schema", "airline"), ",")
  }

  override def loadAirlineDDFWithoutDefault(): DDF = {
    loadTestDataFromFile(getClass.getResource("/airline.csv").getPath, "airlineWithoutDefault",
      config.getValue("schema", "airline"), ",")
  }

  override def loadAirlineDDFWithNA(): DDF = {
    loadTestDataFromFile(getClass.getResource("/airlineWithNA.csv").getPath, "airlineWithNA",
      config.getValue("schema", "airline"), ",")
  }

  override def loadYearNamesDDF(): DDF = {
    loadTestDataFromFile(getClass.getResource("/year_names.csv").getPath, "year_names",
      config.getValue("schema", "year-names"), ",")
  }

}
