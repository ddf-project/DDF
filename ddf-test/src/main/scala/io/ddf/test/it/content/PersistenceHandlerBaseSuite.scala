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
package io.ddf.test.it.content

import java.io.File

import io.ddf.DDF
import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.test.it.BaseSuite
import org.scalatest.Matchers

import scala.collection.JavaConverters._

trait PersistenceHandlerBaseSuite extends BaseSuite with Matchers {

  test("hold namespaces correctly") {
    val ddf: DDF = manager.newDDF

    val namespaces = ddf.getPersistenceHandler.listNamespaces

    namespaces should not be null
    for (namespace <- namespaces.asScala) {
      val ddfs = ddf.getPersistenceHandler.listItems(namespace)
      ddfs should not be null
    }

  }

  test("persist and unpersist a DDF") {
    val ddf: DDF = manager.newDDF
    val uri: PersistenceUri = ddf.persist
    uri.getEngine.toLowerCase() should be(engineName)
    new File(uri.getPath).exists should be (true)
    ddf.unpersist()
  }

}
