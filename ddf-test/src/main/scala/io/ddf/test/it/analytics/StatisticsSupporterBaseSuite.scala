/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except)compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to)writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.ddf.test.it.analytics

import io.ddf.analytics.{AStatisticsSupporter, CategoricalSimpleSummary, NumericSimpleSummary}
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers
import org.scalautils.TolerantNumerics

trait StatisticsSupporterBaseSuite extends BaseSuite with Matchers {

  test("calculate summary") {
    val ddf = loadAirlineDDF()
    val summaries = ddf.getSummary
    summaries.head.max() should be(2010)

    //mean:1084.26 stdev:999.14 var:998284.8 cNA:0 count:31 min:4.0 max:3920.0
    val randomSummary = summaries(9)
    randomSummary.variance() should be(998284.8 +- 1.0)
  }

  test("calculate vector mean") {
    val ddf = loadAirlineDDF()
    ddf.getVectorMean("Year") should not be null
  }


  test("calculate vector cor") {
    val ddf = loadAirlineDDF()
    val cor = ddf.getVectorCor("Year", "Month")
    println(cor)
    cor should not be null
  }

  test("calculate vector covariance") {
    val ddf = loadAirlineDDF()
    val cov = ddf.getVectorCovariance("Year", "Month")
    println(cov)
    cov should not be null
  }

  test("calculate vector variance") {
    val ddf = loadAirlineDDF()
    val variance = ddf.getVectorVariance("Year")
    variance.length should be(2)
  }

  test("calculate vector quantiles") {
    val ddf = loadAirlineDDF()
    val pArray: Array[java.lang.Double] = Array(0.3, 0.5, 0.7)
    val expectedQuantiles: Array[java.lang.Double] = Array(801.0, 1416.0, 1644.0)
    val quantiles: Array[java.lang.Double] = ddf.getVectorQuantiles("DepTime", pArray)
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(20.01)
    for (i <- pArray.indices) {
      quantiles(i) === expectedQuantiles(i)
    }
  }

  test("calculate vector quantiles for double column") {
    val carsDDF = loadMtCarsDDF()
    val pArray: Array[java.lang.Double] = Array(0.0, 0.3, 0.5, 0.3, 1.0)
    val expectedQuantiles: Array[java.lang.Double] = Array(10.4, 15.68, 18.95, 15.68, 33.9)
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)
    val quantiles: Array[java.lang.Double] = carsDDF.getVectorQuantiles("mpg", pArray)
    for (i <- pArray.indices) {
      quantiles(i) === expectedQuantiles(i)
    }
  }

  test("calculate vector histogram") {
    val ddf = loadAirlineDDF()
    val bins: java.util.List[AStatisticsSupporter.HistogramBin] = ddf.getVectorHistogram("ArrDelay", 5)
    bins.size should be(5)
    val first = bins.get(0)
    first.getX should be(-24)
    first.getY should be(10.0)
  }

  test("handle all null column") {
    val ddf = loadCarOwnersNADDF()
    val summaries = ddf.getSummary()
    summaries(1).count() should be(0)
    summaries(1).NACount() should be(4)
    assert(summaries(1).max().isNaN)
    assert(summaries(1).min().isNaN)
    assert(summaries(1).stdev().isNaN)

  }
  // This test is unsupported for ddf-on-jdbc due to the sql statement distinct(column) used in getSimpleSummary
  //ddf-on-jdbc requires distinct on(column) as valid SQL
  ignore("compute simple summary") {
    val airlineDDF = loadAirlineDDFWithoutDefault()
    Array("Year", "Month", "DayofMonth", "UniqueCarrier").foreach(airlineDDF.setAsFactor)
    val simpleSummary = airlineDDF.getStatisticsSupporter.getSimpleSummary

    simpleSummary.find(s => s.getColumnName == "Year").get match {
      case cat: CategoricalSimpleSummary =>
        val categoryValues: java.util.List[String] = cat.getValues
        categoryValues.size should be(3)
        categoryValues should contain allOf("2008", "2009", "2010")
    }

    simpleSummary.find(s => s.getColumnName == "UniqueCarrier").get match {
      case cat: CategoricalSimpleSummary =>
        val categoryValues: java.util.List[String] = cat.getValues
        categoryValues.size should be(1)
        categoryValues should contain("WN")
    }

    simpleSummary.find(s => s.getColumnName == "LateAircraftDelay").get match {
      case num: NumericSimpleSummary =>
        num.getMax should be(72.0)
        num.getMin should be(7.0)
    }

    simpleSummary.find(s => s.getColumnName == "ActualElapsedTime").get match {
      case num: NumericSimpleSummary =>
        num.getMax should be(324.0)
        num.getMin should be(49.0)
    }

    simpleSummary.find(s => s.getColumnName == "DepDelay").get match {
      case num: NumericSimpleSummary =>
        num.getMax should be(94.0)
        num.getMin should be(-4.0)
    }
  }

}
