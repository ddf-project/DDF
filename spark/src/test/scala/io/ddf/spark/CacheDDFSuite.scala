package io.ddf.spark

import io.ddf.spark.content.RepresentationHandler

/**
  * Created by huandao on 6/20/16.
  */
class CacheDDFSuite extends ATestSuite {
  createTableAirline()

  test("cache a ddf") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL")
    ddf.cache()
    assert(ddf.isCache)
    assert(ddf.getRepresentationHandler.asInstanceOf[RepresentationHandler].isCache())

    ddf.Transform.transformUDFWithNames(Array("newcol"), Array("arrdelay + depdelay"), Array("arrdelay", "depdelay"),
      true)
    assert(ddf.isCache)
    assert(ddf.getRepresentationHandler.asInstanceOf[RepresentationHandler].isCache())

    val ddf1 = ddf.Transform.transformUDFWithNames(Array("newcol1"), Array("arrdelay + depdelay"), Array("arrdelay",
      "depdelay"), false)
    assert(!ddf1.isCache)
    assert(!ddf1.getRepresentationHandler.asInstanceOf[RepresentationHandler].isCache())

    ddf.uncache()
    assert(!ddf.isCache)
    assert(!ddf.getRepresentationHandler.asInstanceOf[RepresentationHandler].isCache())
  }
}
