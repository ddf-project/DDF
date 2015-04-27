package io.spark.ddf.content

import io.spark.ddf.ATestSuite

/**
  */
class ListDDFSuite extends ATestSuite {
  createTableMtcars()
  createTableAirline()

  test("test list ddf") {
    val ddf1 = manager.sql2ddf("select * from mtcars")
    val ddf2 = manager.sql2ddf("select * from airline")
    ddf1.setName("mtcars")
    ddf2.setName("airline")
    manager.addDDF(ddf2)

    val listDDF = manager.listDDFs()

    listDDF.foreach {
      ddfinfo => println(s"uri = ${ddfinfo.getUri}; createdTime = ${ddfinfo.getCreatedTime}")
    }

    assert(listDDF.size > 0)
    assert(listDDF(0).getUri != null)
  }
}
