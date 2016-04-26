package io.ddf.spark.content

import java.util

import io.ddf.spark.etl.JoinHandler
import io.ddf.test.it.SparkBaseSuite
import io.ddf.test.it.etl.JoinHandlerBaseSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JoinHandlerSuite extends SparkBaseSuite with JoinHandlerBaseSuite {
  test("test join handler generate select columns") {
    val left_ddf = loadMtCarsDDF()
    val columnId = "lt"
    val suffix = "_l"

    val targetColumns: java.util.List[String] = new util.ArrayList[String]()
    targetColumns.add("col1")
    targetColumns.add("col2")
    targetColumns.add("col3")
    val filterColumns: java.util.List[String] = new util.ArrayList[String]()
    filterColumns.add("col2")
    filterColumns.add("col3")
    filterColumns.add("col4")

    var selectColumn = left_ddf.getJoinsHandler.asInstanceOf[JoinHandler].generateSelectColumns(targetColumns, filterColumns, columnId, suffix)
    selectColumn should be("lt.col1,lt.col2 AS col2_l,lt.col3 AS col3_l")

    targetColumns.clear()
    targetColumns.add("col1")
    targetColumns.add("col2")
    targetColumns.add("col3")
    filterColumns.clear()

    selectColumn = left_ddf.getJoinsHandler.asInstanceOf[JoinHandler].generateSelectColumns(targetColumns, filterColumns, columnId, suffix)
    selectColumn should be("lt.col1,lt.col2,lt.col3")

    targetColumns.clear()

    selectColumn = left_ddf.getJoinsHandler.asInstanceOf[JoinHandler].generateSelectColumns(targetColumns, filterColumns, columnId, suffix)
    selectColumn should be("")

    selectColumn = left_ddf.getJoinsHandler.asInstanceOf[JoinHandler].generateSelectColumns(null, filterColumns, columnId, suffix)
    selectColumn should be("")

    targetColumns.clear()
    targetColumns.add("col1")
    targetColumns.add("col2")
    targetColumns.add("col3")
    selectColumn = left_ddf.getJoinsHandler.asInstanceOf[JoinHandler].generateSelectColumns(targetColumns, null, columnId, suffix)
    selectColumn should be("lt.col1,lt.col2,lt.col3")
  }
}
