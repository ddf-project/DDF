package io.spark.ddf.content

import io.spark.ddf.ATestSuite

/**
 */
class GetDDFSuite extends ATestSuite {

  createTableAirline()
  test("test GetDDF") {
    val ddf = manager.sql2ddf("select * from airline")
    manager.setDDFName(ddf, "awesome_ddf")
    val ddf1 = manager.getDDFByURI(ddf.getUri)
    assert(ddf1 != null)
    assert(ddf1.getNumRows == ddf.getNumRows)
  }
}
