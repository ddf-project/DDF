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

import java.lang.Boolean
import java.util

import io.ddf.DDF
import io.ddf.analytics.Summary
import io.ddf.content.Schema.ColumnType
import io.ddf.exception.DDFException
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers

import scala.collection.JavaConversions._

trait TransformationHandlerBaseSuite extends BaseSuite with Matchers {

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

  test("test sort ascending single column") {
    val smithsData: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
    val columns = new util.ArrayList[String]()
    columns.add("variable")
    val ascending = new util.ArrayList[Boolean]()
    ascending.add(true)
    val newDDF: DDF = smithsData.Transform.sort(columns, ascending)

    val data = newDDF.VIEWS.head(newDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}
    arr.get(0)(0) should be("John")
    arr.get(0)(1) should be ("age")
    arr.get(0)(2) should be ("33.0")

    arr.get(1)(0) should be ("Mary")
    arr.get(1)(1) should be ("age")
    arr.get(1)(2) should be ("33.0")

    arr.get(6)(0) should be ("John")
    arr.get(6)(1) should be ("weight")
    arr.get(6)(2) should be ("90.0")

    arr.get(7)(0) should be ("Mary")
    arr.get(7)(1) should be ("weight")
    arr.get(7)(2) should be ("null")
  }

  test("test sort multiple columns") {
    val smithsData: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
    val columns = new util.ArrayList[String]()
    columns.add("variable")
    columns.add("value")
    val ascending = new util.ArrayList[Boolean]()
    ascending.add(true) // ascending
    ascending.add(false) // descending
    val newDDF: DDF = smithsData.Transform.sort(columns, ascending)

    val data = newDDF.VIEWS.head(newDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}

    arr.get(2)(0) should be("John")
    arr.get(2)(1) should be("height")
    arr.get(2)(2) should be("1.87")

    arr.get(3)(0) should be("Mary")
    arr.get(3)(1) should be("height")
    arr.get(3)(2) should be("1.54")

    arr.get(6)(0) should be("John")
    arr.get(6)(1) should be("weight")
    arr.get(6)(2) should be("90.0")

    arr.get(7)(0) should be("Mary")
    arr.get(7)(1) should be("weight")
    arr.get(7)(2) should be("null")
  }

  test("test sort multiple columns with no sort order") {
    val smithsData: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
    val columns = new util.ArrayList[String]()
    columns.add("subject")
    columns.add("variable")
    val newDDF: DDF = smithsData.Transform.sort(columns, new util.ArrayList[Boolean]())

    val data = newDDF.VIEWS.head(newDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}

    arr.get(0)(0) should be("John")
    arr.get(0)(1) should be("age")
    arr.get(0)(2) should be("33.0")

    arr.get(1)(0) should be("John")
    arr.get(1)(1) should be("height")
    arr.get(1)(2) should be("1.87")

    arr.get(6)(0) should be("Mary")
    arr.get(6)(1) should be("time")
    arr.get(6)(2) should be("1.0")

    arr.get(7)(0) should be("Mary")
    arr.get(7)(1) should be("weight")
    arr.get(7)(2) should be("null")
  }

  test("test sort descending with more sort order than column") {
    val smithsData: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
    val columns = new util.ArrayList[String]()
    columns.add("subject")
    columns.add("variable")
    val ascending = new util.ArrayList[Boolean]()
    ascending.add(false)
    ascending.add(false)
    ascending.add(true)
    ascending.add(false)
    val newDDF: DDF = smithsData.Transform.sort(columns, ascending)

    val data = newDDF.VIEWS.head(newDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}

    arr.get(0)(0) should be("Mary")
    arr.get(0)(1) should be("weight")
    arr.get(0)(2) should be("null")

    arr.get(1)(0) should be("Mary")
    arr.get(1)(1) should be("time")
    arr.get(1)(2) should be("1.0")

    arr.get(6)(0) should be("John")
    arr.get(6)(1) should be("height")
    arr.get(6)(2) should be("1.87")

    arr.get(7)(0) should be("John")
    arr.get(7)(1) should be("age")
    arr.get(7)(2) should be("33.0")
  }

  test("test sort multiple columns empty columns") {
    intercept[DDFException] {
      val smithsData: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
      smithsData.Transform.sort(new util.ArrayList[String](), new util.ArrayList[Boolean]())
    }
  }

  test("test sort unknown column") {
    intercept[DDFException] {
      val smithsData: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
      val columns = new util.ArrayList[String]()
      columns.add("don't exist")
      smithsData.Transform.sort(columns, new util.ArrayList[Boolean]())
    }
  }

  test("test sort inPlace false") {
    val ddf: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
    val columns = new util.ArrayList[String]()
    columns.add("variable")
    columns.add("value")
    val ascending = new util.ArrayList[Boolean]()
    ascending.add(true) // ascending
    ascending.add(false) // descending

    // test inPlace false
    val inPlace = Boolean.FALSE
    val transformedDDF2: DDF = ddf.Transform.sort(columns, ascending, inPlace)
    ddf should not be null
    transformedDDF2 should not be null
    transformedDDF2.getUUID.equals(ddf.getUUID) should be(false)
  }

  test("test sort inPlace true") {
    val ddf: DDF = loadSmithsDDF().VIEWS.project("subject, variable, value")
    val columns = new util.ArrayList[String]()
    columns.add("variable")
    columns.add("value")
    val ascending = new util.ArrayList[Boolean]()
    ascending.add(true) // ascending
    ascending.add(false) // descending

    // test inPlace false
    val inPlace = Boolean.TRUE
    val transformedDDF2: DDF = ddf.Transform.sort(columns, ascending, inPlace)
    ddf should not be null
    transformedDDF2 should not be null
    transformedDDF2.getUUID.equals(ddf.getUUID) should be(true)
  }

  test("test flatten column of array type") {
    val ddf: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    ddf.setMutable(false)
    val ddfWithArrayTypeColumn: DDF = ddf.Transform.transformUDF("arrCol = array(1.234, 5.678, 9.123)")
    val flattenedDDF: DDF = ddfWithArrayTypeColumn.Transform.flattenArrayTypeColumn("arrCol")

    flattenedDDF.getNumColumns should be(12)
    flattenedDDF.getColumnName(8) should be("arrCol")
    flattenedDDF.getColumnName(9) should be("arrCol_c0")

    val element = flattenedDDF.VIEWS.project("arrCol_c2").VIEWS.head(1).get(0)
    element.toDouble should be(9.123)

    val ddfWithArrayTypeColumnStr: DDF = ddf.Transform.transformUDF("arrColStr = array('day', 'month', 'year')")
    val flattenedDDFString: DDF = ddfWithArrayTypeColumnStr.Transform.flattenArrayTypeColumn("arrColStr")

    flattenedDDFString.getNumColumns should be(12)
    flattenedDDFString.getColumnName(8) should be("arrColStr")
    flattenedDDFString.getColumnName(9) should be("arrColStr_c0")

    val elementStr = flattenedDDFString.VIEWS.project("arrColStr_c1").VIEWS.head(1).get(0)
    elementStr should be("month")
  }

  test("test flatten column of array type inPlace False") {
    val inPlace = false
    val ddf: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    val ddfWithArrayTypeColumn: DDF = ddf.Transform.transformUDF("arrCol = array(1.234, 5.678, 9.123)")
    val flattenedDDF: DDF = ddfWithArrayTypeColumn.Transform.flattenArrayTypeColumn("arrCol", inPlace)

    ddfWithArrayTypeColumn should not be null
    ddfWithArrayTypeColumn.getUUID should not equal flattenedDDF.getUUID
    flattenedDDF.getColumnName(8) should be("arrCol")
    flattenedDDF.getColumnName(9) should be("arrCol_c0")
    flattenedDDF.getColumnName(10) should be("arrCol_c1")
    flattenedDDF.getColumnName(11) should be("arrCol_c2")
  }

  test("test flatten column of array type inPlace True") {
    val inPlace = true
    val ddf: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime")
    val ddfWithArrayTypeColumn: DDF = ddf.Transform.transformUDF("arrCol = array(1.234, 5.678, 9.123)")
    val flattenedDDF: DDF = ddfWithArrayTypeColumn.Transform.flattenArrayTypeColumn("arrCol", inPlace)
    
    ddfWithArrayTypeColumn should not be null
    ddfWithArrayTypeColumn.getUUID should equal(flattenedDDF.getUUID)
    flattenedDDF.getColumnName(8) should be("arrCol")
    flattenedDDF.getColumnName(9) should be("arrCol_c0")
    flattenedDDF.getColumnName(10) should be("arrCol_c1")
    flattenedDDF.getColumnName(11) should be("arrCol_c2")
  }
}
