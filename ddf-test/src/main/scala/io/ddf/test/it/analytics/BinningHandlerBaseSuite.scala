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

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnClass}
import io.ddf.exception.DDFException
import io.ddf.test.it.BaseSuite
import io.ddf.types.AggregateTypes
import org.scalatest.Matchers

import scala.collection.JavaConverters._

trait BinningHandlerBaseSuite extends BaseSuite with Matchers {

  test("Bin by equal interval") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALINTERVAL", 2, null, true, true, true)
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getColumnClass shouldBe ColumnClass.FACTOR
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get().size shouldBe 2
    newDDF.getColumn(monthColumnLabel).getOptionalFactor.getLevels.isPresent shouldBe true
    newDDF.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get should contain allOf ("[1,6]", "(6,11]")
    newDDF.getNumRows shouldBe 31
  }

  test("Bin by equal frequency") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALFREQ", 2, null, true, true)
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getColumnClass shouldBe ColumnClass.FACTOR
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get().size() shouldBe 2

    val levelCounts: java.util.Map[String, Integer] = newDDF.getSchemaHandler.computeLevelCounts(monthColumnLabel)
    levelCounts.get("[1,1]") should be(17)
    levelCounts.values().asScala.reduce(_ + _) should be(31)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size 31
  }

  ignore("bin by custom interval") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "custom", 0, Array[Double](2, 4, 6, 8), true, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn("Month")
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    // {"[2,4]"=6, "(4,6]"=3, "(6,8]"=2}
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get().size should be(3)
    val levelCounts: java.util.Map[String, Integer] = newDDF.getSchemaHandler.computeLevelCounts(monthColumnLabel)
    levelCounts.get("[2,4]") should be(6)
    levelCounts.get("(4,6]") should be(3)
    levelCounts.get("(6,8]") should be(2)
    levelCounts.values().asScala.reduce(_ + _) should be(11)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size 11
  }

  ignore("bin by equal interval excluding highest") {
    val monthColumnLabel: String = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALINTERVAL", 2, null, true, false)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"[1,6)" = 24,"[6,11)" = 6}
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevelMap.size should be(2)
    val levelCounts: java.util.Map[String, Integer] = newDDF.getSchemaHandler.computeLevelCounts(monthColumnLabel)
    levelCounts.get("[1,6)") should be(24)
    levelCounts.get("[6,11)") should be(6)
    levelCounts.values().asScala.reduce(_ + _) should be(30)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size 30
  }

  ignore("bin by equal interval excluding lowest") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALINTERVAL", 2, null, false, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"(1,6]" = 9,"(6,11]" = 5}
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get().size should be(2)
    val levelCounts: java.util.Map[String, Integer] = newDDF.getSchemaHandler.computeLevelCounts(monthColumnLabel)
    levelCounts.get("(1,6]") should be(9)
    levelCounts.get("(6,11]") should be(5)
    levelCounts.values().asScala.reduce(_ + _) should be(14)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size 14
  }

  ignore("bin by equal interval excluding lowest and highest") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALINTERVAL", 2, null, false, false)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"(1,6)" = 7,"(6,11)" = 4}
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get.size should be(2)
    val levelCounts: java.util.Map[String, Integer] = newDDF.getSchemaHandler.computeLevelCounts(monthColumnLabel)
    levelCounts.get("(1,6)") should be(7)
    levelCounts.get("(6,11)") should be(4)
    levelCounts.values().asScala.reduce(_ + _) should be(11)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size 11
  }

  /*
  test("Binning to integer") {
    val ddf: DDF = loadAirlineDDF()
    val ddf1: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, false, true, true)
    ddf1.getSchemaHandler.getColumn("dayofweek").getType shouldBe Schema.ColumnType.INT
    var ret: AggregateTypes.AggregationResult = ddf1.xtabs("dayofweek, COUNT(*)")
    ret.keySet should have size 1
    ret.keySet should not contain "1"
    ret.get("0")(0) shouldBe 31

    val thrown = intercept[DDFException] {
      ddf.binning("dayofweek", "EQUALFREQ", 2, null, false, true, true)
    }
    assert(thrown.getMessage === "Breaks must be unique: [4.0, 4.0, 4.0]")

    val ddf2: DDF = ddf.binning("arrdelay", "EQUALFREQ", 2, null, false, true, true)
    ddf2.getSchemaHandler.getColumn("arrdelay").getType shouldBe Schema.ColumnType.INT
    ret = ddf2.xtabs("arrdelay, COUNT(*)")
    ret.keySet should have size 2
    ret.keySet should contain allOf ("0", "1")

    var ddf3: DDF = ddf.binning("month", "custom", 0, Array[Double](1, 2, 4, 6, 8, 10, 12), false, true, true)
    ddf3.getSchemaHandler.getColumn("month").getType shouldBe Schema.ColumnType.INT
    ret = ddf3.xtabs("month, COUNT(*)")
    ret.keySet should have size 6
    ret.keySet should contain allOf ("0", "1", "2", "3", "4", "5")

    ddf3 = ddf.binning("month", "custom", 0, Array[Double](2, 4, 6, 8), false, true, true)
    ddf2.getSchemaHandler.getColumn("month").getType shouldBe Schema.ColumnType.INT
    ret = ddf3.xtabs("month, COUNT(*)")
    ret.keySet should have size 4
    ret.keySet should contain allOf ("0", "1", "2", "null")
  }

  test("Binning to labels") {
    val ddf: DDF = loadAirlineDDF()
    val ddf1: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, true)
    ddf1.getColumn("dayofweek").getColumnClass shouldBe ColumnClass.FACTOR
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.isPresent shouldBe true
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.get should contain allOf ("[3.996,4]", "(4,4.004]")

    val thrown = intercept[DDFException] {
      ddf.binning("dayofweek", "EQUALFREQ", 2, null, true, true, true)
    }
    assert(thrown.getMessage === "Breaks must be unique: [4.0, 4.0, 4.0]")

    var ddf2: DDF = ddf.binning("arrdelay", "EQUALFREQ", 2, null, true, true, true)
    ddf2.getColumn("arrdelay").getColumnClass shouldBe ColumnClass.FACTOR
    ddf2.getColumn("arrdelay").getOptionalFactor.getLevels.isPresent shouldBe true
    ddf2.getColumn("arrdelay").getOptionalFactor.getLevels.get should contain allOf ("[-24,4]", "(4,80]")

    ddf2 = ddf.binning("arrdelay", "EQUALFREQ", 2, null, true, false, false)
    ddf2.getColumn("arrdelay").getColumnClass shouldBe ColumnClass.FACTOR
    ddf2.getColumn("arrdelay").getOptionalFactor.getLevels.isPresent shouldBe true
    ddf2.getColumn("arrdelay").getOptionalFactor.getLevels.get should contain allOf ("[-24,4)", "[4,80]")

    var ddf3: DDF = ddf.binning("month", "custom", 0, Array[Double](1, 2, 4, 6, 8, 10, 12), true, true, true)
    ddf3.getColumn("month").getColumnClass shouldBe ColumnClass.FACTOR
    ddf3.getColumn("month").getOptionalFactor.getLevels.isPresent shouldBe true
    ddf3.getColumn("month").getOptionalFactor.getLevels.get should contain allOf ("[1,2]", "(2,4]", "(4,6]", "(6,8]", "(10,12]")

    ddf3 = ddf.binning("month", "custom", 0, Array[Double](2, 4, 6, 8), true, true, true)
    ddf3.getColumn("month").getColumnClass shouldBe ColumnClass.FACTOR
    ddf3.getColumn("month").getOptionalFactor.getLevels.isPresent shouldBe true
    ddf3.getColumn("month").getOptionalFactor.getLevels.get should contain allOf ("[2,4]", "(4,6]", "(6,8]")
  }

  test("Binning to labels with right include lowest") {
    val ddf: DDF = loadAirlineDDF()
    var ddf1: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, true)
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.get should contain allOf ("[3.996,4]", "(4,4.004]")
    var levelCounts = ddf1.getSchemaHandler.computeLevelCounts("dayofweek")
    levelCounts should not contain "(4,4.004]"
    levelCounts.get("[3.996,4]") shouldBe 31

    ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, false)
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.get should contain allOf ("[3.996,4)", "[4,4.004]")
    levelCounts = ddf1.getSchemaHandler.computeLevelCounts("dayofweek")
    levelCounts should not contain "[3.996,4)"
    levelCounts.get("[4,4.004]") shouldBe 31

    ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, false, false)
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.get should contain allOf ("[3.996,4)", "[4,4.004]")
    levelCounts = ddf1.getSchemaHandler.computeLevelCounts("dayofweek")
    levelCounts should not contain "[3.996,4)"
    levelCounts.get("[4,4.004]") shouldBe 31

    ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, false, true)
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.get should contain allOf ("[3.996,4]", "(4,4.004]")
    levelCounts = ddf1.getSchemaHandler.computeLevelCounts("dayofweek")
    levelCounts should not contain "(4,4.004]"
    levelCounts.get("[3.996,4]") shouldBe 31
  }

  test("Binning to labels precision") {
    val ddf: DDF = loadAirlineDDF()
    val ddf1: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, true, 1)
    ddf1.getColumn("dayofweek").getOptionalFactor.getLevels.get should contain allOf ("[3.996,4]", "(4,4.004]")

    val levelCounts: util.Map[String, Integer] = ddf1.getSchemaHandler.computeLevelCounts("dayofweek")
    levelCounts should not contain "(4,4.004]"
    levelCounts.get("[3.996,4]") shouldBe 31
  }

  test("Binning invalid cases") {
    val ddf: DDF = loadAirlineDDF()
    val thrown = intercept[DDFException] {
      ddf.binning("month", "custom", 0, Array[Double](4, 2, 6, 8), true, true, true, 1)
    }
    assert(thrown.getMessage === "Please enter increasing breaks")
  }
  */
}
