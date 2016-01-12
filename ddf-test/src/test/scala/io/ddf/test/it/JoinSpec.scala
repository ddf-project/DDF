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

import java.util.Collections

import io.ddf.DDF
import io.ddf.etl.Types.JoinType
import org.scalatest.Matchers

trait JoinSpec extends BaseSpec with Matchers {

  feature("Join") {
    scenario("inner join tables") {
      val ddf: DDF = loadAirlineDDF()
      val ddf2: DDF = loadYearNamesDDF()
      val joinedDDF = ddf.join(ddf2, null, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val colNames = joinedDDF.getSchema.getColumnNames
      colNames.contains("Year") || colNames.contains("year") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.contains("Name") || colNames.contains("r_name") || colNames.contains("name") should be(true)
      joinedDDF.getNumRows should be(2)
      joinedDDF.getNumColumns should be(31)
    }

    scenario("left semi join tables") {
      val ddf: DDF = loadAirlineDDF()
      val ddf2: DDF = loadYearNamesDDF()
      val joinedDDF = ddf.join(ddf2, JoinType.LEFTSEMI, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val colNames = joinedDDF.getSchema.getColumnNames
      colNames.contains("Year") || colNames.contains("year") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.contains("Name") || colNames.contains("r_name") || colNames.contains("name") should be(false)
      joinedDDF.getNumRows should be(2)
      joinedDDF.getNumColumns should be(29)
    }

    scenario("left outer join tables") {
      val ddf: DDF = loadAirlineDDF()
      val ddf2: DDF = loadYearNamesDDF()
      val joinedDDF = ddf.join(ddf2, JoinType.LEFT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val colNames = joinedDDF.getSchema.getColumnNames
      colNames.contains("Year") || colNames.contains("year") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.contains("Name") || colNames.contains("r_name") || colNames.contains("name") should be(true)
      joinedDDF.getNumRows should be(3)
      joinedDDF.getNumColumns should be(31)
    }

    scenario("right outer join tables") {
      val ddf: DDF = loadAirlineDDF()
      val ddf2: DDF = loadYearNamesDDF()
      val joinedDDF = ddf.join(ddf2, JoinType.RIGHT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val colNames = joinedDDF.getSchema.getColumnNames
      colNames.contains("Year") || colNames.contains("year") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.contains("Name") || colNames.contains("r_name") || colNames.contains("name") should be(true)
      joinedDDF.getNumRows should be(4)
      joinedDDF.getNumColumns should be(31)
    }

    scenario("full outer join tables") {
      val ddf: DDF = loadAirlineDDF()
      val ddf2: DDF = loadYearNamesDDF()
      val joinedDDF = ddf.join(ddf2, JoinType.FULL, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
      val colNames = joinedDDF.getSchema.getColumnNames
      colNames.contains("Year") || colNames.contains("year") should be(true)
      //check if the names from second ddf have been added to the schema
      colNames.contains("Name") || colNames.contains("r_name") || colNames.contains("name") should be(true)
      joinedDDF.getNumRows should be(5)
      joinedDDF.getNumColumns should be(31)
    }
  }

}
