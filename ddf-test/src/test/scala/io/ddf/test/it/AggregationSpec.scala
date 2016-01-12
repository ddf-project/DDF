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
package io.ddf.test.it

import io.ddf.types.AggregateTypes.AggregateFunction
import org.scalatest.Matchers
import scala.collection.JavaConversions._

trait AggregationSpec extends BaseSpec with Matchers {

  feature("Aggregation") {
    scenario("calculate simple aggregates") {
      val ddf = loadAirlineDDF()
      val aggregateResult = ddf.aggregate("Year, Month, min(ArrDelay), max(DepDelay)")
      val result: Array[Double] = aggregateResult.get("2008\t3")
      result.length should be(2)

      val colAggregate = ddf.getAggregationHandler.aggregateOnColumn(AggregateFunction.MAX, "Year")
      colAggregate should be(2010)
    }

    scenario("group data") {
      val ddf = loadAirlineDDF()
      val l1: java.util.List[String] = List("DayofMonth")
      val l2: java.util.List[String] = List("avg(DepDelay)")
      val avgDelayByDay = ddf.groupBy(l1, l2)
      avgDelayByDay.getColumnNames.map(col => col.toLowerCase()) should contain("dayofmonth")
      avgDelayByDay.getColumnNames.size() should be(2)
      val rows = avgDelayByDay.sql("select * from @this", "").getRows
      rows.head.split("\t").head.toDouble should be(21.0 +- 1.0)
    }

    scenario("group and aggregate)2 steps") {
      val ddf = loadAirlineDDF()
      val ddf2 = ddf.getAggregationHandler.groupBy(List("DayofMonth"))
      val result = ddf2.getAggregationHandler.agg(List("mean=avg(ArrDelay)"))
      result.getColumnNames.map(col => col.toLowerCase) should (contain("mean") and contain("dayofmonth"))
      val rows = result.sql("select * from @this", "").getRows
      rows.head.split("\t").head.toDouble should be(9.0 +- 1.0)
    }

    scenario("throw an error on aggregate without groups") {
      val airlineDDF = loadAirlineDDF()
      val ddf = manager.sql2ddf("select * from airline", engineName)
      intercept[Exception] {
        ddf.getAggregationHandler.agg(List("mean=avg(ArrDelay)"))
      }
    }

    scenario("calculate correlation") {
      val ddf = loadAirlineDDF()
      //0.8977184691827954
      ddf.correlation("ArrDelay", "DepDelay") should be(0.89 +- 1)
    }
  }

}
