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
import io.ddf.etl.IHandleMissingData.{Axis, NAChecking}
import io.ddf.exception.DDFException
import io.ddf.test.it.BaseSuite
import io.ddf.types.AggregateTypes.AggregateFunction
import org.scalatest.Matchers

import scala.collection.JavaConversions._

trait MissingDataHandlerSuite extends BaseSuite with Matchers {

  test("drop all rows with NA values") {
    val result = loadAirlineDDFWithNA().dropNA()
    result.getNumRows should be(9)
  }

  test("keep all the rows") {
    val result = loadAirlineDDFWithNA().getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ALL, 0, null)
    result.getNumRows should be(31)
  }

  test("keep all the rows when drop threshold is high") {
    val result = loadAirlineDDFWithNA().getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ALL, 10, null)
    result.getNumRows should be(31)
  }

  test("throw an exception when drop threshold > columns") {
    intercept[DDFException] {
      loadAirlineDDFWithNA().getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ANY, 31, null)
    }
  }

  test("drop all columns with NA values") {
    val result = loadAirlineDDFWithNA().dropNA(Axis.COLUMN)
    result.getNumColumns should be(22)
  }

  test("drop all columns with NA values with load table") {
    val result = loadAirlineDDFWithNA().dropNA(Axis.COLUMN)
    result.getNumColumns should be(22)
  }

  test("keep all the columns") {
    val result = loadAirlineDDFWithNA().getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ALL, 0, null)
    result.getNumColumns should be(29)
  }

  test("keep most(24) columns when drop threshold is high(20)") {
    val result = loadAirlineDDFWithNA().getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ALL, 20, null)
    result.getNumColumns should be(24)
  }

  test("throw an exception when drop threshold > rows") {
    intercept[DDFException] {
      loadAirlineDDFWithNA().getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ANY, 40, null)
    }
  }

  test("fill by value") {
    val ddf = loadAirlineDDF()
    val ddf1: DDF = ddf.VIEWS.project(List("Year", "LateAircraftDelay"))
    val filledDDF: DDF = ddf1.fillNA("0")
    val annualDelay = filledDDF.aggregate("Year, sum(LateAircraftDelay)").get("2008")(0)
    annualDelay should be(282.0 +- 0.1)
  }

  ignore("fill by dictionary") {
    val ddf = loadAirlineDDF()
    val ddf1: DDF = ddf.VIEWS.project(List("Year", "SecurityDelay", "LateAircraftDelay"))
    val dict: Map[String, String] = Map("Year" -> "2000", "SecurityDelay" -> "0", "LateAircraftDelay" -> "1")
    val filledDDF = ddf1.getMissingDataHandler.fillNA(null, null, 0, null, dict, null)
    val annualDelay = filledDDF.aggregate("Year, sum(LateAircraftDelay)").get("2008")(0)
    annualDelay should be(282.0 +- 0.1)
  }

  test("fill by aggregate function") {
    val ddf = loadAirlineDDF()
    val ddf1: DDF = ddf.VIEWS.project(List("Year", "SecurityDelay", "LateAircraftDelay"))
    val result = ddf1.getMissingDataHandler.fillNA(null, null, 0, AggregateFunction.MEAN, null, null)
    result should not be null
  }

}
