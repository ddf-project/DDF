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

import io.ddf.exception.DDFException
import io.ddf.test.it.BaseSuite
import io.ddf.types.AggregateTypes.AggregateFunction
import org.scalatest.Matchers

import scala.collection.JavaConversions._

trait AggregationHandlerBaseSuite extends BaseSuite with Matchers {

  test("throw an error on aggregate without groups") {
    val ddf = loadAirlineDDF(useCache = true)
    intercept[Exception] {
      ddf.getAggregationHandler.agg(List("mean=avg(ArrDelay)"))
    }
  }

  test("calculate simple aggregates") {
    val ddf = loadAirlineDDF(useCache = true)
    val aggregateResult1 = ddf.aggregate("Year, Month, min(ArrDelay), max(DepDelay)")
    val result: Array[Double] = aggregateResult1.get("2008\t3")
    result.length should be(2)

    val colAggregate = ddf.getAggregationHandler.aggregateOnColumn(AggregateFunction.MAX, "Year")
    colAggregate should be(2010)

    val aggregateResult2 = ddf.aggregate("year, month, avg(depdelay), stddev(arrdelay)")
    aggregateResult2.size() should be(13)
    ddf.VIEWS.head(5).size should be(5)
    ddf.VIEWS.project(List("year", "month", "deptime")).getNumColumns should be(3)
  }

  test("group data") {
    val ddf = loadAirlineDDF(useCache = true)
    val l1: java.util.List[String] = List("DayofMonth")
    val l2: java.util.List[String] = List("avg(DepDelay)")
    val avgDelayByDay = ddf.groupBy(l1, l2)
    avgDelayByDay.getColumnNames.map(col => col.toLowerCase()) should contain("dayofmonth")
    avgDelayByDay.getColumnNames.size() should be(2)
    val rows = avgDelayByDay.sql("select * from @this", "").getRows
    rows.head.split("\t").head.toDouble should be(21.0 +- 1.0)
  }

  test("group and aggregate 2 steps") {
    val ddf = loadAirlineDDF(useCache = true)
    val ddf2 = ddf.getAggregationHandler.groupBy(List("DayofMonth"))
    val result = ddf2.getAggregationHandler.agg(List("mean=avg(ArrDelay)"))
    result.getColumnNames.map(col => col.toLowerCase) should (contain("mean") and contain("dayofmonth"))
    val rows = result.sql("select * from @this", "").getRows
    rows.head.split("\t").head.toDouble should be(9.0 +- 1.0)
  }

  test("calculate correlation") {
    val ddf = loadAirlineDDF(useCache = true)
    ddf.correlation("ArrDelay", "DepDelay") should be(0.89 +- 1)
  }

  test("Proper error message for non-existent columns") {
    val ddf = loadAirlineDDF(useCache = true)

    val thrown = intercept[DDFException] {
      ddf.groupBy(List("Year1"), List("count(*)"))
    }
    assert(thrown.getMessage === "Non-existent column: Year1")

    val thrown2 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("avg(arrdelay1)"))
    }
    assert(thrown2.getMessage === "Non-existent column: arrdelay1")

    val thrown3 = intercept[DDFException] {
      ddf.aggregate("count(*),Year1")
    }
    assert(thrown3.getMessage === "Non-existent column: Year1")

    val thrown4 = intercept[DDFException] {
      ddf.aggregate("avg(arrdelay1),Year")
    }
    assert(thrown4.getMessage === "Non-existent column: arrdelay1")
  }

  test("Proper error message for expressions") {
    val ddf = loadAirlineDDF(useCache = true)

    val thrown1 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("arrdelay+"))
    }
    assert(thrown1.getMessage === "Column or Expression with invalid syntax: 'arrdelay+'")

    val thrown2 = intercept[DDFException] {
      ddf.aggregate("arrdelay+,Year")
    }
    assert(thrown2.getMessage === "Column or Expression with invalid syntax: 'arrdelay+'")
  }

  test("Proper error message for undefined function") {
    val ddf = loadAirlineDDF(useCache = true)

    val thrown1 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("aaa(arrdelay)"))
    }
    assert(thrown1.getMessage === "undefined function aaa")
  }

  test("Proper error message for wrong Hive UDF usage") {
    val ddf = loadAirlineDDF(useCache = true)

    val thrown1 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("substring('aaaa')"))
    }
    assert(thrown1.getMessage.contains("No matching method for class org.apache.hadoop.hive.ql.udf.UDFSubstr with " +
      "(string). Possible choices: _FUNC_(binary, int)  _FUNC_(binary, int, int)  " +
      "_FUNC_(string, int)  _FUNC_(string, int, int)"))

    val thrown2 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("to_utc_timestamp(1,1,1)"))
    }
    assert(thrown2.getMessage.contains("The function to_utc_timestamp requires two argument, got 3"))
  }

  test("Proper error message for expressions or columns that contain invalid characters") {
    val ddf = loadAirlineDDF(useCache = true)

    val thrown1 = intercept[DDFException] {
      ddf.groupBy(List("Year@"), List("avg(arrdelay)"))
    }
    assert(thrown1.getMessage === "Expressions or columns containing invalid character @: Year@")

    val thrown2 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("avg(arrdelay@)"))
    }
    assert(thrown2.getMessage === "Expressions or columns containing invalid character @: avg(arrdelay@)")

    val thrown3 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("avg(arrdelay @)"))
    }
    assert(thrown3.getMessage === "Column or Expression with invalid syntax: 'avg(arrdelay @)'")

    val thrown31 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("@avg(arrdelay)"))
    }
    assert(thrown31.getMessage === "Column or Expression with invalid syntax: '@avg(arrdelay)'")

    val thrown4 = intercept[DDFException] {
      ddf.groupBy(List("Year @"), List("avg(arrdelay)"))
    }
    assert(thrown4.getMessage === "Column or Expression with invalid syntax: 'Year @'")

    val thrown5 = intercept[DDFException] {
      ddf.aggregate("Year@,avg(arrdelay1)")
    }
    assert(thrown5.getMessage === "Expressions or columns containing invalid character @: Year@")

    val thrown6 = intercept[DDFException] {
      ddf.aggregate("Year,avg(arrdelay@)")
    }
    assert(thrown6.getMessage === "Expressions or columns containing invalid character @: AVG(arrdelay@)")
  }

  test("Proper error message for new columns that contain invalid characters") {
    val ddf = loadAirlineDDF(useCache = true)

    val thrown1 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("newcol@=avg(arrdelay)"))
    }
    assert(thrown1.getMessage === "Expressions or columns containing invalid character @: newcol@=avg(arrdelay)")
  }

  test("Proper error message for new columns that exist") {
    val ddf = loadAirlineDDF(useCache = true)

    val groupped = ddf.groupBy(List("Year"), List("arrdelay=avg(arrdelay)"))
    assert(groupped.getColumnNames.contains("arrdelay"))

    val thrown1 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("Year=avg(arrdelay)"))
    }
    assert(thrown1.getMessage === "New column name in aggregation cannot be a group by column: Year")

    val thrown2 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("Year = avg(arrdelay)"))
    }
    assert(thrown2.getMessage === "New column name in aggregation cannot be a group by column: Year")

    val thrown3 = intercept[DDFException] {
      ddf.groupBy(List("Year"), List("foo=avg(arrdelay)", "foo=sum(arrdelay)"))
    }
    assert(thrown3.getMessage === "Duplicated column name in aggregations: foo")
  }

}
