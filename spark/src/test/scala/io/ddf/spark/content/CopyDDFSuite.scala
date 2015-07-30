package io.ddf.spark.content

import io.ddf.spark.ATestSuite
/**
 * Created by huandao on 7/20/15.
 */
class CopyDDFSuite extends ATestSuite {
  createTableMtcars()
  createTableAirline()
  test("copy ddf") {
    val ddf1 = manager.sql2ddf("select * from mtcars")
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
      col => ddf1.getSchemaHandler.setAsFactor(col)
    }

    val ddf2 = ddf1.copy()
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
      col => assert(ddf2.getSchemaHandler.getColumn(col).getOptionalFactor != null)
    }
    assert(ddf1.getNumRows == ddf2.getNumRows)
    assert(ddf1.getNumColumns == ddf2.getNumColumns)
  }
}
