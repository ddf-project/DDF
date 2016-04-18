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
package io.ddf.test.it

import io.ddf.util.ConfigHandler
import io.ddf.{DDF, DDFManager}
import org.scalatest.FunSuite

/*
 * This is the base class that contains all methods to load test data
 * and other utilities / configuration
 * Any engine must override and implement these methods to make the tests
 * run properly
 */
trait BaseSuite extends FunSuite {

  val config: ConfigHandler = new ConfigHandler("ddf-conf", "ddf_spec.ini")
  val engineName: String
  val manager: DDFManager

  def loadMtCarsDDF(): DDF
  def loadAirlineDDF(): DDF
  def loadAirlineDDFWithoutDefault(): DDF
  def loadAirlineDDFWithNA(): DDF
  def loadYearNamesDDF(): DDF

}
