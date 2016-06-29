package io.ddf.spark


class CacheDDFSuite extends ATestSuite {
  createTableAirline()

  test("cache a ddf") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL")
    ddf.getRepresentationHandler.cache(false)
    assert(ddf.getRepresentationHandler.isCached)

    ddf.Transform.transformUDFWithNames(Array("newcol"), Array("arrdelay + depdelay"), Array("arrdelay", "depdelay"),
      true)
    assert(ddf.getRepresentationHandler.isCached)

    val ddf1 = ddf.Transform.transformUDFWithNames(Array("newcol1"), Array("arrdelay + depdelay"), Array("arrdelay",
      "depdelay"), false)
    assert(!ddf1.getRepresentationHandler.isCached)

    ddf.getRepresentationHandler.uncache(false)
    assert(!ddf.getRepresentationHandler.isCached)
  }
}
