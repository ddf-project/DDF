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

import io.ddf.DDF
import org.scalatest.Matchers

trait ViewSpec extends BaseSpec with Matchers {

  feature("View") {
    ignore("project after remove columns ") {
      val ddf1 = loadAirlineDDF()
      val columns: java.util.List[String] = new java.util.ArrayList()
      columns.add("Year")
      columns.add("Month")
      columns.add("Deptime")
      val ddf2 = loadAirlineDDF()
      val newddf1: DDF = ddf1.VIEWS.removeColumn("Year")
      newddf1.getNumColumns should be(28)
      val newddf3: DDF = ddf2.VIEWS.removeColumns(columns)
      newddf3.getNumColumns should be(26)
    }


    scenario("test sample") {
      val ddf = loadMtCarsDDF()
      val sample = ddf.VIEWS.getRandomSample(10)
      sample.get(0)(0).toString.toDouble should not be sample.get(1)(0).toString.toDouble
      sample.get(1)(0).toString.toDouble should not be sample.get(2)(0).toString.toDouble
      sample.get(2)(0).toString.toDouble should not be sample.get(3)(0).toString.toDouble
      sample.size should be(10)
    }

    //This is not implemented for ddf-on-jdbc
    scenario("test sample with percentage") {
      val ddf = loadAirlineDDF()
      val sample = ddf.VIEWS.getRandomSample(0.5, false, 1)
      sample.VIEWS.head(3) should have size 3
    }


    scenario("get top 3 rows") {
      loadAirlineDDF()
      val sample = manager.sql2ddf("SELECT Month from airline", engineName)
      sample.VIEWS.head(3) should have size 3
    }
  }

}
