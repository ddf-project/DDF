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

  test("transform scale min max all columns") {
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val scaledDDF: DDF = originalDDF.Transform.transformScaleMinMax
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    val scaledDDFSummary: Array[Summary] = scaledDDF.getSummary
    scaledDDFSummary(0).min should be(0)
    scaledDDFSummary(0).max should be(1)
    scaledDDFSummary(1).min should be(0)
    scaledDDFSummary(1).max should be(1)
    scaledDDFSummary(4).min should be(0)
    scaledDDFSummary(4).max should be(1)
    // The following column has min == max, hence should not be scaled
    scaledDDFSummary(2).min should be(3)
    scaledDDFSummary(2).max should be(3)
    scaledDDFSummary(3).min should be(4)
    scaledDDFSummary(3).max should be(4)
    // Not numeric columns
    scaledDDFSummary(8) should be(null)
    scaledDDFSummary(10) should be(null)

    val data = scaledDDF.VIEWS.head(scaledDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}

    arr.get(0)(0).toDouble should be(0)
    arr.get(1)(0).toDouble should be(0.5)
    arr.get(2)(0).toDouble should be(1)

    arr.get(0)(2).toDouble should be(3)
    arr.get(1)(2).toDouble should be(3)
    arr.get(2)(2).toDouble should be(3)

    arr.get(0)(4).toDouble should be((2003 - originalDDFSummary(4).min) / (originalDDFSummary(4).max-originalDDFSummary(4).min))
    arr.get(1)(4).toDouble should be((754 - originalDDFSummary(4).min) / (originalDDFSummary(4).max-originalDDFSummary(4).min))
    arr.get(2)(4).toDouble should be((628 - originalDDFSummary(4).min) / (originalDDFSummary(4).max-originalDDFSummary(4).min))

    arr.get(0)(8) should be("WN")
    arr.get(1)(8) should be("WN")
    arr.get(2)(8) should be("WN")
  }

  test("transform scale min max subset of columns") {
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val columnsToScale = List("Year", "DayofMonth", "TailNum", "UnknownColumn", null, "DayofMonth")
    val scaledDDF: DDF = originalDDF.Transform.transformScaleMinMax(columnsToScale, false)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    val scaledDDFSummary: Array[Summary] = scaledDDF.getSummary
    scaledDDFSummary(0).min should be(0)
    scaledDDFSummary(0).max should be(1)
    scaledDDFSummary(1).min should be(1)
    scaledDDFSummary(1).max should be(11)
    scaledDDFSummary(4).min should be(617)
    scaledDDFSummary(4).max should be(2107)
    // The following column has min == max, hence should not be scaled
    scaledDDFSummary(2).min should be(3)
    scaledDDFSummary(2).max should be(3)
    // Not numeric columns
    scaledDDFSummary(8) should be(null)
    scaledDDFSummary(10) should be(null)

    val data = scaledDDF.VIEWS.head(scaledDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}
    arr.get(0)(0).toDouble should be(0)
    arr.get(1)(0).toDouble should be(0.5)
    arr.get(2)(0).toDouble should be(1)

    // not requested to be scaled
    arr.get(0)(1).toDouble should be(1)
    arr.get(1)(1).toDouble should be(1)
    arr.get(2)(1).toDouble should be(3)

    // requested to be scaled but min == max
    arr.get(0)(2).toDouble should be(3)
    arr.get(1)(2).toDouble should be(3)
    arr.get(2)(2).toDouble should be(3)

    // not requested to be scaled
    arr.get(0)(4).toDouble should be(2003)
    arr.get(1)(4).toDouble should be(754)
    arr.get(2)(4).toDouble should be(628)

    arr.get(0)(8) should be("WN")
    arr.get(1)(8) should be("WN")
    arr.get(2)(8) should be("WN")
  }

  test("transform scale min max inPlace false") {
    val inPlace = false
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val columnsToScale = List("Year", "DayofMonth", "TailNum", "UnknownColumn", null, "DayofMonth")
    val scaledDDF: DDF = originalDDF.Transform.transformScaleMinMax(columnsToScale, inPlace)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    scaledDDF.getUUID should not be(originalDDF.getUUID)
  }

  test("transform scale min max inPlace true") {
    val inPlace = true
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val scaledDDF: DDF = originalDDF.Transform.transformScaleMinMax(List(), inPlace)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    scaledDDF.getUUID should be(originalDDF.getUUID)
  }

  test("transform scale standard all columns") {
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val scaledDDF: DDF = originalDDF.Transform.transformScaleStandard(null, true)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    val scaledDDFSummary: Array[Summary] = scaledDDF.getSummary
    BigDecimal(scaledDDFSummary(0).mean).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(0)
    BigDecimal(scaledDDFSummary(0).stdev).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(1)
    BigDecimal(scaledDDFSummary(1).mean).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(0)
    BigDecimal(scaledDDFSummary(1).stdev).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(1)
    BigDecimal(scaledDDFSummary(4).mean).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(0)
    BigDecimal(scaledDDFSummary(4).stdev).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(1)
    // The following column has min == max, hence should not be scaled
    scaledDDFSummary(2).mean should be(3)
    scaledDDFSummary(2).stdev should be(0)
    scaledDDFSummary(3).mean should be(4)
    scaledDDFSummary(3).stdev should be(0)
    // Not numeric columns
    scaledDDFSummary(8) should be(null)
    scaledDDFSummary(10) should be(null)

    val data = scaledDDF.VIEWS.head(scaledDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}

    arr.get(0)(0).toDouble should be((2008 - originalDDFSummary(0).mean) / originalDDFSummary(0).stdev)
    arr.get(1)(0).toDouble should be((2009 - originalDDFSummary(0).mean) / originalDDFSummary(0).stdev)
    arr.get(2)(0).toDouble should be((2010 - originalDDFSummary(0).mean) / originalDDFSummary(0).stdev)

    arr.get(0)(2).toDouble should be(3)
    arr.get(1)(2).toDouble should be(3)
    arr.get(2)(2).toDouble should be(3)

    arr.get(0)(4).toDouble should be((2003 - originalDDFSummary(4).mean) / originalDDFSummary(4).stdev)
    arr.get(1)(4).toDouble should be((754 - originalDDFSummary(4).mean) / originalDDFSummary(4).stdev)
    arr.get(2)(4).toDouble should be((628 - originalDDFSummary(4).mean) / originalDDFSummary(4).stdev)

    arr.get(0)(8) should be("WN")
    arr.get(1)(8) should be("WN")
    arr.get(2)(8) should be("WN")
  }

  test("transform scale standard subset of columns") {
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val columnsToScale = List("Year", "DayofMonth", "TailNum", "UnknownColumn", null, "DayofMonth")
    val scaledDDF: DDF = originalDDF.Transform.transformScaleStandard(columnsToScale, false)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    val scaledDDFSummary: Array[Summary] = scaledDDF.getSummary
    BigDecimal(scaledDDFSummary(0).mean).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(0)
    BigDecimal(scaledDDFSummary(0).stdev).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble should be(1)
    scaledDDFSummary(1).mean should be(originalDDFSummary(1).mean)
    scaledDDFSummary(1).stdev should be(originalDDFSummary(1).stdev)
    // The following column has stddev == 0, hence wasn't rescaled
    scaledDDFSummary(2).mean should be(originalDDFSummary(2).mean)
    scaledDDFSummary(2).stdev should be(originalDDFSummary(2).stdev)
    // This column wasn't in the list to be scaled
    scaledDDFSummary(4).mean should be(originalDDFSummary(4).mean)
    scaledDDFSummary(4).stdev should be(originalDDFSummary(4).stdev)
    // Not numeric columns
    scaledDDFSummary(8) should be(null)
    scaledDDFSummary(10) should be(null)

    val data = scaledDDF.VIEWS.head(scaledDDF.getNumRows.toInt)
    val arr = data.map{str => str.split("\\t")}

    arr.get(0)(0).toDouble should be((2008 - originalDDFSummary(0).mean) / originalDDFSummary(0).stdev)
    arr.get(1)(0).toDouble should be((2009 - originalDDFSummary(0).mean) / originalDDFSummary(0).stdev)
    arr.get(2)(0).toDouble should be((2010 - originalDDFSummary(0).mean) / originalDDFSummary(0).stdev)

    arr.get(0)(2).toDouble should be(3)
    arr.get(1)(2).toDouble should be(3)
    arr.get(2)(2).toDouble should be(3)

    arr.get(0)(4).toDouble should be(2003)
    arr.get(1)(4).toDouble should be(754)
    arr.get(2)(4).toDouble should be(628)

    arr.get(0)(8) should be("WN")
    arr.get(1)(8) should be("WN")
    arr.get(2)(8) should be("WN")
  }

  test("transform scale standard inPlace false") {
    val inPlace = false
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val columnsToScale = List("Year", "DayofMonth", "TailNum", "UnknownColumn", null, "DayofMonth")
    val scaledDDF: DDF = originalDDF.Transform.transformScaleStandard(columnsToScale, inPlace)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    scaledDDF.getUUID should not be(originalDDF.getUUID)
  }

  test("transform scale standard inPlace true") {
    val inPlace = true
    val originalDDF: DDF = loadAirlineDDF().VIEWS.project("Year", "Month", "DayofMonth", "DayofWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum")
    val originalDDFSummary: Array[Summary] = originalDDF.getSummary

    val scaledDDF: DDF = originalDDF.Transform.transformScaleStandard(null, inPlace)
    scaledDDF should not be null
    scaledDDF.getNumRows should be(31)
    scaledDDF.getNumColumns should be(11)

    scaledDDF.getUUID should be(originalDDF.getUUID)
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
