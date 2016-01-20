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

import io.ddf.datasource.{DataSourceDescriptor, DataSourceURI, JDBCDataSourceCredentials, JDBCDataSourceDescriptor}
import io.ddf.misc.Config
import io.ddf.util.ConfigHandler
import io.ddf.{DDF, DDFManager}
import org.scalatest.FeatureSpec

import scala.reflect.runtime.{universe => ru}

trait BaseSpec extends FeatureSpec {

  val THIS_TRAIT = "io.ddf.ddfTest.BaseSpec"

  val configHandler: ConfigHandler = new ConfigHandler("ddf-conf", "ddf_spec.ini")

  val engineName: String = configHandler.getValue("global", "engine")

  val baseSpec = getValue("baseSpec")

  val manager: DDFManager = {
    if (baseSpec == THIS_TRAIT) {
      if (engineName == "aws" || engineName == "jdbc" || engineName == "postgres")
        DDFManager.get(DDFManager.EngineType.fromString(engineName), getEngineDescriptor(engineName))
      else
        DDFManager.get(DDFManager.EngineType.fromString(engineName))
    }
    else
      getDef("manager").asInstanceOf[DDFManager]
  }

  def getEngineDescriptor(engine: String) = {
    if (baseSpec == THIS_TRAIT) {
      val USER = "jdbcUser"
      val PASSWORD = "jdbcPassword"
      val URL = "jdbcUrl"
      val user = Config.getValue(engine, USER)
      val password = Config.getValue(engine, PASSWORD)
      val jdbcUrl = Config.getValue(engine, URL)
      val dataSourceURI = new DataSourceURI(jdbcUrl)
      val credentials = new JDBCDataSourceCredentials(user, password)
      new JDBCDataSourceDescriptor(dataSourceURI, credentials, null)
    }
    else
      getDef("getEngineDescriptor").asInstanceOf[DataSourceDescriptor]
  }

  def loadMtCarsDDF(): DDF = {
    if (baseSpec == THIS_TRAIT) {
      val ddfName: String = "mtcars"
      loadCSVIfNotExists(ddfName, s"/$ddfName")
    }
    else getDef("loadMtCarsDDF").asInstanceOf[DDF]
  }

  private def loadCSVIfNotExists(ddfName: String,
                                 fileName: String): DDF = {
    if (baseSpec == THIS_TRAIT) {
      try {
        manager.sql2ddf(s"SELECT * FROM $ddfName", engineName)
      } catch {
        case e: Exception =>
          if (engineName != "flink")
            manager.sql(getValue("drop-" + ddfName), engineName)

          manager.sql(getValue("create-" + ddfName), engineName)

          val filePath = getClass.getResource(fileName).getPath

          manager.sql(getValue("load-" + ddfName).replace("$filePath", s"$filePath"), engineName)

          manager.sql2ddf(s"SELECT * FROM $ddfName", engineName)
      }
    }
    else getDef("loadCSVIfNotExists", ddfName, fileName).asInstanceOf[DDF]
  }

  def getValue(key: String) = {
    val value = configHandler.getValue(engineName, key)
    if (value == null || value == "") configHandler.getValue("global", key)
    else value
  }

  def loadAirlineDDF(): DDF = {
    if (baseSpec == THIS_TRAIT) {
      val ddfName = "airline"
      loadCSVIfNotExists(ddfName, s"/$ddfName.csv")
    }
    else getDef("loadAirlineDDF").asInstanceOf[DDF]
  }

  def loadAirlineDDFWithoutDefault(): DDF = {
    if (baseSpec == THIS_TRAIT) {
      val ddfName = "airlineWithoutDefault"
      loadCSVIfNotExists(ddfName, "/airline.csv")
    }
    else getDef("loadAirlineDDFWithoutDefault").asInstanceOf[DDF]
  }

  def loadAirlineNADDF(): DDF = {
    if (baseSpec == THIS_TRAIT) {
      val ddfName = "airlineWithNA"
      loadCSVIfNotExists(ddfName, s"/$ddfName.csv")
    }
    else getDef("loadAirlineNADDF").asInstanceOf[DDF]
  }


  def loadYearNamesDDF(): DDF = {
    if (baseSpec == THIS_TRAIT) {
      val ddfName = "year_names"
      loadCSVIfNotExists(ddfName, s"/$ddfName.csv")
    }
    else getDef("loadYearNamesDDF").asInstanceOf[DDF]
  }

  private def getDef(name: String, args: Any*) = {
    val runtimeMirror = ru.runtimeMirror(Class.forName(baseSpec).getClassLoader)
    val moduleSymbol = runtimeMirror.moduleSymbol(Class.forName(baseSpec))

    val targetMethod = moduleSymbol.typeSignature
      .members
      .filter(x => x.isMethod && x.name.toString == name)
      .head
      .asMethod

    runtimeMirror.reflect(runtimeMirror.reflectModule(moduleSymbol).instance)
      .reflectMethod(targetMethod)(args: _*)

  }
}
