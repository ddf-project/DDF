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

import java.util.Collections

import io.ddf.DDF
import io.ddf.etl.Types.JoinType
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers

trait JoinHandlerBaseSuite extends BaseSuite with Matchers {

  test("inner join mtcars and carowner") {
    val left_ddf = loadMtCarsDDF()
    val right_ddf = loadCarOwnersDDF()
    val ddf = left_ddf.join(right_ddf, JoinType.INNER, Collections.singletonList("cyl"), null, null)
    ddf.getNumRows should be(25)
  }

  test("inner join tables") {
    val ddf: DDF = loadAirlineDDF()
    val ddf2: DDF = loadYearNamesDDF()
    val joinedDDF = ddf.join(ddf2, JoinType.INNER, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") || colNames.contains("year") should be(true)
    colNames.contains("Name") || colNames.contains("name_r") || colNames.contains("name") should be(true)
    joinedDDF.getNumColumns should be(31)
    joinedDDF.getNumRows should be(30)
  }

  test("left semi join tables") {
    val ddf: DDF = loadAirlineDDF()
    val ddf2: DDF = loadYearNamesDDF()
    val joinedDDF = ddf.join(ddf2, JoinType.LEFTSEMI, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") || colNames.contains("year") should be(true)
    colNames.contains("Name") || colNames.contains("name_r") || colNames.contains("name") should be(false)
    joinedDDF.getNumColumns should be(29)
    joinedDDF.getNumRows should be(30)
  }

  test("left outer join tables") {
    val ddf: DDF = loadAirlineDDF()
    val ddf2: DDF = loadYearNamesDDF()
    val joinedDDF = ddf.join(ddf2, JoinType.LEFT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") || colNames.contains("year") should be(true)
    colNames.contains("Name") || colNames.contains("name_r") || colNames.contains("name") should be(true)
    joinedDDF.getNumColumns should be(31)
    joinedDDF.getNumRows should be(31)
  }

  test("right outer join tables") {
    val ddf: DDF = loadAirlineDDF()
    val ddf2: DDF = loadYearNamesDDF()
    val joinedDDF = ddf.join(ddf2, JoinType.RIGHT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") || colNames.contains("year") should be(true)
    colNames.contains("Name") || colNames.contains("name_r") || colNames.contains("name") should be(true)
    joinedDDF.getNumColumns should be(31)
    joinedDDF.getNumRows should be(32)
  }

  test("full outer join tables") {
    val ddf: DDF = loadAirlineDDF()
    val ddf2: DDF = loadYearNamesDDF()
    val joinedDDF = ddf.join(ddf2, JoinType.FULL, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") || colNames.contains("year") should be(true)
    colNames.contains("Name") || colNames.contains("name_r") || colNames.contains("name") should be(true)
    joinedDDF.getNumColumns should be(31)
    joinedDDF.getNumRows should be(33)
  }

  test("inner join suffix") {
    val left_ddf = loadMtCarsDDF()
    val right_ddf = loadCarOwnersDDF()
    val ddf = left_ddf.join(right_ddf, JoinType.INNER, Collections.singletonList("cyl"),null,null, "_left", "_right")
    val colNames = ddf.getSchema.getColumnNames
    colNames.contains("cyl_left") should be(true)
    colNames.contains("cyl_right") should be(true)
    colNames.contains("disp_left") should be(true)
    colNames.contains("disp_right") should be(true)
  }
}
