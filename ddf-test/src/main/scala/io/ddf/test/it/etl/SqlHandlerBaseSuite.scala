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
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers

trait SqlHandlerBaseSuite extends BaseSuite with Matchers {

  test("create table and load data from file") {
    val ddf = loadAirlineDDF()
    ddf.getColumnNames should have size 29

    //MetaDataHandler
    ddf.getNumRows should be(31)

    //StatisticsComputer
    val summaries = ddf.getSummary
    summaries.head.max() should be(2010)

    //mean:1084.26 stdev:999.14 var:998284.8 cNA:0 count:31 min:4.0 max:3920.0
    val randomSummary = summaries(9)
    randomSummary.variance() >= 998284
  }

  test("run a simple sql command") {
    val ddf = loadAirlineDDF()
    val ddf1 = manager.sql2ddf("select Year,Month from airline", engineName)
    ddf1.getNumColumns should be(2)
  }

  test("run a sql command with where") {
    val ddf = loadAirlineDDF()
    val ddf1 = manager.sql2ddf("select Year,Month from airline where Year > 2008 AND Month > 1", engineName)
    ddf1.getNumRows should be(1)
    ddf1.getNumColumns should be(2)
  }

  test("run a sql command with a join") {
    val ddf: DDF = loadAirlineDDF()
    val ddf2: DDF = loadYearNamesDDF()
    val ddf3 = manager.sql2ddf("select Year,Month from airline join year_names on (Year = Year_num) ", engineName)
    val ddf4 = manager.sql2ddf("select Year,Month from airline left join year_names on (Year = Year_num) ", engineName)
  }

  test("run a sql command with a join and where") {
    val ddf = loadAirlineDDF()
    val ddf4 = manager.sql2ddf("select Year,Month from airline left join year_names on (Year = Year_num) where " +
      "Year_num > 2008 ", engineName)

  }

  test("run a sql command with an orderby") {
    val ddf = loadAirlineDDF()
    val ddf4 = manager.sql2ddf("select Year,Month from airline order by Year DESC", engineName)
    ddf4.getNumColumns should be(2)
  }

  test("run a sql command with an orderby and limit") {
    val ddf = loadAirlineDDF()
    val ddf4 = manager.sql2ddf("select Year,Month from airline order by Year DESC limit 2", engineName)
    ddf4.getNumRows should be(2)
    ddf4.getNumColumns should be(2)
  }

  test("run a sql command with a group-by and order-by and limit") {
    val ddf = loadAirlineDDF()
    val ddf4 = manager.sql2ddf("select Year,Month,Count(Cancelled) from airline group by Year,Month order by Year " +
      "DESC limit 5", engineName)
  }

}
