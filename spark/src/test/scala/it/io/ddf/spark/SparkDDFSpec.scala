/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package it.io.ddf.spark

import io.ddf.test.it._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkDDFSpec extends BaseSpec with StatisticsSpec with BinningSpec with AggregationSpec with
JoinSpec with MissingDataSpec with PersistenceSpec with SchemaSpec with SqlSpec
with TransformationSpec with ViewSpec {

  def runMultiple(names: String) = {
    names.split(",").foreach(name => this.execute(name))
  }

  feature("copy") {
    //This feature is unsupported for ddf-on-jdbc

    scenario("factor columns should be copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col =>
          assert(ddf2.getSchemaHandler.getColumn(col).getOptionalFactor != null)
      }
    }

    scenario("all rows are copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      assert(ddf1.getNumRows == ddf2.getNumRows)
    }

    scenario("all columns are copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      assert(ddf1.getNumColumns == ddf2.getNumColumns)
    }

    ignore("name is not copied") {
      val ddf1 = loadMtCarsDDF()
      Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
        col => ddf1.getSchemaHandler.setAsFactor(col)
      }

      val ddf2 = ddf1.copy()
      assert(ddf1.getName != ddf2.getName)
    }
  }
}