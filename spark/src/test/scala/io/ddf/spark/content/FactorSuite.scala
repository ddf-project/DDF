package io.ddf.spark.content

import io.ddf.content.Schema.{ColumnClass, ColumnType}
import io.ddf.spark.ATestSuite

/**
  */
class FactorSuite extends ATestSuite {
  createTableMtcars()
  createTableAirlineWithNA()

  test("test get factors on DDF with TablePartition") {
    val ddf = manager.sql2ddf("select * from mtcars")
    val schemaHandler = ddf.getSchemaHandler
    Array(7, 8, 9, 10).foreach {
      idx => schemaHandler.setAsFactor(idx)
    }
    schemaHandler.computeFactorLevelsAndLevelCounts()
    val cols = Array(7, 8, 9, 10).map {
      idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
    }
    assert(cols(0).getOptionalFactor.getLevelCounts.get("1") === 14)
    assert(cols(0).getOptionalFactor.getLevelCounts.get("0") === 18)
    assert(cols(1).getOptionalFactor.getLevelCounts.get("1") === 13)
    assert(cols(2).getOptionalFactor.getLevelCounts.get("4") === 12)

    assert(cols(2).getOptionalFactor.getLevelCounts.get("3") === 15)
    assert(cols(2).getOptionalFactor.getLevelCounts.get("5") === 5)
    assert(cols(3).getOptionalFactor.getLevelCounts.get("1") === 7)
    assert(cols(3).getOptionalFactor.getLevelCounts.get("2") === 10)
  }

  test("test get factor with long column") {
    val ddf = manager.sql2ddf("select mpg, cast(cyl as bigint) as cyl from mtcars")
    ddf.getSchemaHandler.setAsFactor("cyl")
    ddf.getSchemaHandler.computeFactorLevelsAndLevelCounts()
    assert(ddf.getSchemaHandler.getColumn("cyl").getType == ColumnType.LONG)
    assert(ddf.getSchemaHandler.getColumn("cyl").getColumnClass == ColumnClass.FACTOR)
    assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("4") == 11)
    assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("6") == 7)
    assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("8") == 14)
  }

  test("test get factors for DDF with RDD[Array[Object]]") {
    val ddf = manager.sql2ddf("select * from mtcars")
    //    ddf.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])

    val schemaHandler = ddf.getSchemaHandler

    Array(7, 8, 9, 10).foreach {
      idx => schemaHandler.setAsFactor(idx)
    }
    schemaHandler.computeFactorLevelsAndLevelCounts()

    val cols2 = Array(7, 8, 9, 10).map {
      idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
    }

    assert(cols2(0).getOptionalFactor.getLevelCounts.get("1") === 14)
    assert(cols2(0).getOptionalFactor.getLevelCounts.get("0") === 18)
    assert(cols2(1).getOptionalFactor.getLevelCounts.get("1") === 13)
    assert(cols2(2).getOptionalFactor.getLevelCounts.get("4") === 12)

    assert(cols2(2).getOptionalFactor.getLevelCounts.get("3") === 15)
    assert(cols2(2).getOptionalFactor.getLevelCounts.get("5") === 5)
    assert(cols2(3).getOptionalFactor.getLevelCounts.get("1") === 7)
    assert(cols2(3).getOptionalFactor.getLevelCounts.get("2") === 10)
  }

  test("test NA handling") {
    val ddf = manager.sql2ddf("select * from airlineWithNA")
    val schemaHandler = ddf.getSchemaHandler

    Array(0, 8, 16, 17, 24, 25).foreach {
      idx => schemaHandler.setAsFactor(idx)
    }
    schemaHandler.computeFactorLevelsAndLevelCounts()

    val cols = Array(0, 8, 16, 17, 24, 25).map {
      idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
    }
    assert(cols(0).getOptionalFactor.getLevels.contains("2008"))
    assert(cols(0).getOptionalFactor.getLevels.contains("2010"))
    assert(cols(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
    assert(cols(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

    assert(cols(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

    assert(cols(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
    assert(cols(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
    assert(cols(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

    assert(cols(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
    assert(cols(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
    assert(cols(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
    assert(cols(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
    assert(cols(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

    assert(cols(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
    assert(cols(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)

    val ddf2 = manager.sql2ddf("select * from airlineWithNA")
    //    ddf2.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])

    val schemaHandler2 = ddf2.getSchemaHandler
    Array(0, 8, 16, 17, 24, 25).foreach {
      idx => schemaHandler2.setAsFactor(idx)
    }
    schemaHandler2.computeFactorLevelsAndLevelCounts()

    val cols2 = Array(0, 8, 16, 17, 24, 25).map {
      idx => schemaHandler2.getColumn(schemaHandler2.getColumnName(idx))
    }

    assert(cols2(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
    assert(cols2(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

    assert(cols2(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

    assert(cols2(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
    assert(cols2(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
    assert(cols2(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

    assert(cols2(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
    assert(cols2(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
    assert(cols2(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
    assert(cols2(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
    assert(cols2(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

    assert(cols2(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
    assert(cols2(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)
  }
}
