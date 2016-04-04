package io.ddf.spark.content

import io.ddf.content.Schema.{ColumnClass, ColumnType}
import io.ddf.spark.ATestSuite
import scala.collection.JavaConversions._

/**
  */
class FactorSuite extends ATestSuite {
  createTableMtcars()
  createTableAirlineWithNA()

<<<<<<< HEAD
=======
  test("test get factors on DDF with TablePartition") {
    val ddf = manager.sql2ddf("select * from mtcars", false)
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

>>>>>>> 6ab8241fdb1d1ab371de77dd68da96fa5d4a31e2
  test("test get factor with long column") {
    val ddf = manager.sql2ddf("select mpg, cast(cyl as bigint) as cyl from mtcars", false)
    ddf.getSchemaHandler.setAsFactor("cyl")
    val map = ddf.getSchemaHandler.computeLevelCounts(Array("cyl"))
    assert(ddf.getSchemaHandler.getColumn("cyl").getType == ColumnType.BIGINT)
    assert(ddf.getSchemaHandler.getColumn("cyl").getColumnClass == ColumnClass.FACTOR)
<<<<<<< HEAD
    assert(map.get("cyl").get("4") == 11)
    assert(map.get("cyl").get("6") == 7)
    assert(map.get("cyl").get("8") == 14)
=======
    assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("4") == 11)
    assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("6") == 7)
    assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("8") == 14)
  }

  test("test set factor for string columns") {
    val ddf = manager.sql2ddf("select * from airlineWithNA", false)
    assert(ddf.getSchemaHandler.getColumn("Origin").getType == ColumnType.STRING)
    assert(ddf.getSchemaHandler.getColumn("Origin").getColumnClass == ColumnClass.CHARACTER)
    ddf.getSchemaHandler.setFactorLevelsForStringColumns(ddf.getSchemaHandler.getColumns.map{col => col.getName}.toArray)
    ddf.getSchemaHandler.computeFactorLevelsAndLevelCounts()
    assert(ddf.getSchemaHandler.getColumn("Origin").getType == ColumnType.STRING)
    assert(ddf.getSchemaHandler.getColumn("Origin").getColumnClass == ColumnClass.FACTOR)
    assert(ddf.getSchemaHandler.getColumn("Origin").getOptionalFactor.getLevelCounts.size() == 3)

>>>>>>> 6ab8241fdb1d1ab371de77dd68da96fa5d4a31e2
  }

  test("test get factors for DDF with RDD[Array[Object]]") {
    val ddf = manager.sql2ddf("select * from mtcars", false)
    //    ddf.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])

    val schemaHandler = ddf.getSchemaHandler

    val columns = Array("vs", "am", "gear", "carb")
    columns.foreach {
      idx => schemaHandler.setAsFactor(idx)
    }
    val factorMap = schemaHandler.computeLevelCounts(columns)

    assert(factorMap.get("vs").get("1") === 14)
    assert(factorMap.get("vs").get("0") === 18)
    assert(factorMap.get("am").get("1") === 13)
    assert(factorMap.get("gear").get("4") === 12)

    assert(factorMap.get("gear").get("3") === 15)
    assert(factorMap.get("gear").get("5") === 5)
  }

  test("test NA handling") {
    val ddf = manager.sql2ddf("select * from airlineWithNA", false)
    val schemaHandler = ddf.getSchemaHandler
    val columnNames = Array(0, 8, 16, 17, 24, 25).map {
      idx => schemaHandler.getColumnName(idx)
    }
    columnNames.foreach{
      col => schemaHandler.setAsFactor(col)
    }
    val factorMap = schemaHandler.computeLevelCounts(columnNames)
    val cols = Array(0, 8, 16, 17, 24, 25).map {
      idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
    }
    val factor = cols(0).getOptionalFactor
    factor.setLevels(schemaHandler.computeFactorLevels(cols(0).getName))

<<<<<<< HEAD
    val levels = cols(0).getOptionalFactor.getLevels.get().map {
      l => l.toString
=======
    assert(cols(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
    assert(cols(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)

    val ddf2 = manager.sql2ddf("select * from airlineWithNA", false)
    //    ddf2.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])

    val schemaHandler2 = ddf2.getSchemaHandler
    Array(0, 8, 16, 17, 24, 25).foreach {
      idx => schemaHandler2.setAsFactor(idx)
>>>>>>> 6ab8241fdb1d1ab371de77dd68da96fa5d4a31e2
    }
    assert(levels.contains("2008"))
    assert(levels.contains("2010"))
    assert(factorMap.get(columnNames(0)).get("2008") === 28.0)
    assert(factorMap.get(columnNames(0)).get("2010") === 1.0)
    assert(factorMap.get(columnNames(3)).get("MCO") === 3.0)
    assert(factorMap.get(columnNames(3)).get("TPA") === 3.0)
    assert(factorMap.get(columnNames(3)).get("JAX") === 1.0)
    assert(factorMap.get(columnNames(3)).get("LAS") === 3.0)
    assert(factorMap.get(columnNames(3)).get("BWI") === 10.0)
    assert(factorMap.get(columnNames(5)).get("0") === 9.0)
    assert(factorMap.get(columnNames(4)).get("3") === 1.0)
  }

  test("preserving factor in projection") {
    val ddf = manager.sql2ddf("select * from mtcars", "SparkSQL")
    //    ddf.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])
    val schemaHandler = ddf.getSchemaHandler
    val factorColumns = Array("vs", "am", "gear", "carb")
    factorColumns.foreach {
      column =>
        val factor = schemaHandler.setAsFactor(column)
//        factor.setLevels(schemaHandler.computeFactorLevels(column))
    }
//    schemaHandler.computeLevelCounts(factorColumns)
//    schemaHandler.computeFactorLevelsAndLevelCounts()
    val projectedColumns = Array("wt", "qsec", "vs", "am", "gear", "carb")
    val projectedDDF = ddf.VIEWS.project(projectedColumns: _*)
    factorColumns.foreach{
      col =>
        val column = projectedDDF.getColumn(col)
        assert(column.getOptionalFactor != null)
    }
  }
}
