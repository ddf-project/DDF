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
package io.ddf.spark.test

import io.ddf.test.it._
import io.ddf.test.it.analytics.{StatisticsSupporterBaseSuite, BinningHandlerBaseSuite, AggregationHandlerBaseSuite}
import io.ddf.test.it.content.{PersistenceHandlerBaseSuite, ViewsHandlerBaseSuite, SchemaHandlerBaseSuite}
import io.ddf.test.it.etl.{MissingDataHandlerBaseSuite, JoinHandlerBaseSuite, TransformationHandlerBaseSuite, SqlHandlerBaseSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkDDFTests extends SparkBaseSuite with StatisticsSupporterBaseSuite with BinningHandlerBaseSuite
with AggregationHandlerBaseSuite with MissingDataHandlerBaseSuite
with PersistenceHandlerBaseSuite with SchemaHandlerBaseSuite with SqlHandlerBaseSuite
with TransformationHandlerBaseSuite with ViewsHandlerBaseSuite {

  def runMultiple(names: String) = {
    names.split(",").foreach(name => this.execute(name))
  }

  test("factor columns should be copied") {
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

  test("all rows are copied") {
    val ddf1 = loadMtCarsDDF()
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
      col => ddf1.getSchemaHandler.setAsFactor(col)
    }

    val ddf2 = ddf1.copy()
    assert(ddf1.getNumRows == ddf2.getNumRows)
  }

  test("all columns are copied") {
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