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
package io.ddf.test.it.etl

import io.ddf.DDF
import io.ddf.analytics.Summary
import io.ddf.content.Schema.ColumnType
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers

import scala.collection.JavaConversions._

trait TransformationHandlerSuite extends BaseSuite with Matchers {

  ignore("transform native Rserve") {
    val relevantData: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    val resultDDF = relevantData.Transform.transformNativeRserve("newcol = DepTime / ArrTime")
    resultDDF should not be null
    resultDDF.getColumnName(29) should be("newcol")
  }

  ignore("transform native map reduce") {

    val mapFuncDef: String = "function(part) { keyval(key=part$Year, val=part$DayofWeek) }"
    val reduceFuncDef: String = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }"
    val relevantData: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    val subset = relevantData.VIEWS.project(List("Year", "DayofWeek"))

    val newDDF: DDF = subset.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef)
    newDDF should not be null
    newDDF.getColumnName(0) should be("key")
    newDDF.getColumnName(1) should be("val")

    newDDF.getColumn("key").getType should be(ColumnType.STRING)
    newDDF.getColumn("val").getType should be(ColumnType.INT)
  }

  //Transform scale min max and transform scale standard currently have functions in ddf core which currently do
  // not work if min and max are equal
  ignore("transform scale min max") {
    val relevantData: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    val newddf0: DDF = relevantData.Transform.transformScaleMinMax
    val summaryArr: Array[Summary] = newddf0.getSummary
    println("result summary is" + summaryArr(0))
    summaryArr(0).min should be < 1.0
    summaryArr(0).max should be(1.0)
  }

  ignore("transform scale standard") {
    val relevantData: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    val newDDF: DDF = relevantData.Transform.transformScaleStandard()
    newDDF.getNumRows should be(31)
    newDDF.getSummary.length should be(8)
  }

}
