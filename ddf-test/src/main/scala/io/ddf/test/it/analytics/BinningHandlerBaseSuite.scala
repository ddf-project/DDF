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

import io.ddf.DDF
import io.ddf.content.Schema.{Column, ColumnClass}
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers

import scala.collection.JavaConverters._

trait BinningHandlerBaseSuite extends BaseSuite with Matchers {

  ignore("bin by equal interval") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALINTERVAL", 2, null, true, true)
    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"[1,6]" = 26,"(6,11]" = 5}
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get().size should be(2)
    val levelCounts: java.util.Map[String, Integer] = newDDF.getSchemaHandler.computeLevelCounts(monthColumnLabel)
    levelCounts.get("[1,6]") should be(26)
    levelCounts.get("(6,11]") should be(5)
    levelCounts.values().asScala.reduce(_ + _) should be(31)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size 31
  }

  ignore("bin by equal frequency") {
    val monthColumnLabel = "Month"
    val newDDF: DDF = loadAirlineDDF().binning(monthColumnLabel, "EQUALFREQ", 2, null, true, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  [1,1] -> 17--(1,11] -> 14
    newDDF.getSchemaHandler.getColumn(monthColumnLabel).getOptionalFactor.getLevels.get().size() should be(2)
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

}
