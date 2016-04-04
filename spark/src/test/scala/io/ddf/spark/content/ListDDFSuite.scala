package io.ddf.spark.content

import io.ddf.spark.ATestSuite

/**
  */
class ListDDFSuite extends ATestSuite {
  createTableMtcars()
  createTableAirline()

  test("test list ddf") {
    val ddf1 = manager.sql2ddf("select * from mtcars", false)
    val ddf2 = manager.sql2ddf("select * from airline", false)
    ddf1.getManager.setDDFName(ddf1, "mtcars")
    ddf2.getManager.setDDFName(ddf2, "airline")
    manager.addDDF(ddf2)

    val listDDF = manager.listDDFs()

    listDDF.foreach {
      ddfinfo => LOG.info(s"name = ${ddfinfo.getName};")
    }

    assert(listDDF.size > 0)
  }
}
