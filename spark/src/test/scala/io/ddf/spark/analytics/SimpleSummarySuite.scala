package io.ddf.spark.analytics

import io.ddf.analytics.{CategoricalSimpleSummary, NumericSimpleSummary}
import io.ddf.spark.ATestSuite

import scala.collection.JavaConversions._

/**
 */
class SimpleSummarySuite extends ATestSuite {
  createTableAirline()
  test("simple summary") {
    val ddf = manager.sql2ddf("select * from airline", false)
    val sqlContext = manager.getHiveContext
    Array("year", "month", "dayofmonth", "uniquecarrier").map{col => ddf.getSchemaHandler.setAsFactor(col)}
    val simpleSummary = ddf.getStatisticsSupporter.getSimpleSummary

    simpleSummary.find(s => s.getColumnName == "year").get match {
      case cat: CategoricalSimpleSummary => assert(cat.getValues.size == 3)
        assert(cat.getValues.exists(s => s=="2008"))
        assert(cat.getValues.exists(s => s=="2009"))
        assert(cat.getValues.exists(s => s=="2010"))
    }
    simpleSummary.find(s => s.getColumnName == "uniquecarrier").get match {
      case cat: CategoricalSimpleSummary => assert(cat.getValues.size == 2)
        assert(cat.getValues()(0) == "WN")
    }

    simpleSummary.find(s => s.getColumnName == "lateaircraftdelay") get match {
      case num: NumericSimpleSummary => assert(num.getMax == 592008.0)
        assert(num.getMin == 7.0)
    }

    simpleSummary.find(s => s.getColumnName == "actualelapsedtime") get match {
      case num: NumericSimpleSummary => assert(num.getMax == 324.0)
        assert(num.getMin == 49.0)
    }

    simpleSummary.find(s => s.getColumnName == "depdelay") get match {
      case num: NumericSimpleSummary => assert(num.getMax == 94.0)
        assert(num.getMin == -4.0)
    }
  }
}
