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

import io.ddf.content.Schema.{Column, ColumnClass, ColumnType}
import org.scalatest.Matchers

import scala.collection.JavaConverters._

trait SchemaSpec extends BaseSpec with Matchers {

  feature("Schema") {
    scenario("get schema") {
      val ddf = loadAirlineDDF()
      ddf.getSchema should not be null
    }

    scenario("get columns") {
      val ddf = loadAirlineDDF()
      val columns: java.util.List[Column] = ddf.getSchema.getColumns
      columns should not be null
      columns.asScala.length should be(29)
      columns.asScala.head.getName.toLowerCase() should be("year")
    }

    scenario("get number of rows") {
      val ddf = loadAirlineDDF()
      ddf.getNumRows should be(31)
    }

    scenario("get columns for sql2ddf create table") {
      val ddf = loadAirlineDDF()
      val columns = ddf.getSchema.getColumns
      columns should not be null
      columns.asScala.length should be(29)
      columns.asScala.head.getName.toLowerCase should be("year")
    }

    scenario("test get factors on DDF ") {
      val ddf = loadMtCarsDDF()
      val schemaHandler = ddf.getSchemaHandler
      Array(7, 8, 9, 10).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()
      val cols = Array(7, 8, 9, 10).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      println("", cols.mkString(","))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("1") === 14)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("0") === 18)
      assert(cols(1).getOptionalFactor.getLevelCounts.get("1") === 13)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("4") === 12)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("3") === 15)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("5") === 5)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("1") === 7)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("2") === 10)
    }

    //TODO support cast
    /*scenario("test get factor with long column"){
      loadMtCarsDDF()
      val ddf = manager.sql2ddf("select mpg, cast(cyl as bigint) as cyl from mtcars",engineName)
      ddf.getSchemaHandler.setAsFactor("cyl")
      ddf.getSchemaHandler.computeFactorLevelsAndLevelCounts()
      assert(ddf.getSchemaHandler.getColumn("cyl").getType == ColumnType.BIGINT)
      assert(ddf.getSchemaHandler.getColumn("cyl").getColumnClass == ColumnClass.FACTOR)
      assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("4") == 11)
      assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("6") == 7)
      assert(ddf.getSchemaHandler.getColumn("cyl").getOptionalFactor.getLevelCounts.get("8") == 14)
    }*/

    scenario("test NA handling") {
      val ddf = loadAirlineNADDF()
      val schemaHandler = ddf.getSchemaHandler

      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()

      val cols = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      assert(cols(0).getOptionalFactor.getLevels.contains("2008"))
      assert(cols(0).getOptionalFactor.getLevels.contains("2010"))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

      assert(cols(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

      assert(cols(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

      assert(cols(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
      assert(cols(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)

      val ddf2 = manager.sql2ddf("select * from airlineWithNA", engineName)
      //    ddf2.getRepresentationHandler.remove(classOf[RDD[_]], classOf[TablePartition])

      val schemaHandler2 = ddf2.getSchemaHandler
      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler2.setAsFactor(idx)
      }
      schemaHandler2.computeFactorLevelsAndLevelCounts()

      val cols2 = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler2.getColumn(schemaHandler2.getColumnName(idx))
      }

      assert(cols2(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
      assert(cols2(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

      assert(cols2(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

      assert(cols2(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

      assert(cols2(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

      assert(cols2(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
      assert(cols2(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)
    }
  }

}
