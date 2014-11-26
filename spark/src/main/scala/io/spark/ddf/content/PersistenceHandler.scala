package io.spark.ddf.content

import io.basic.ddf.content.{PersistenceHandler => BPersistenceHandler}
import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.util.Utils
import io.ddf.util.Utils.JsonSerDes
import org.apache.spark.sql.SchemaRDD
import io.spark.ddf.SparkDDFManager
import io.ddf.content.IHandlePersistence.IPersistible

/**
 * author: daoduchuan
 */

class PersistenceHandler(ddf: DDF) extends BPersistenceHandler(ddf) {

  override def persist(doOverwrite: Boolean): PersistenceUri = {
    val dataFile = this.getDataFileName()
    val schemaFile = this.getSchemaFileName()

    Utils.writeToFile(schemaFile, JsonSerDes.serialize(this.getDDF.getSchema) + "\n")
    val schemaRDD = ddf.getRepresentationHandler.get(classOf[SchemaRDD]).asInstanceOf[SchemaRDD]
    schemaRDD.saveAsParquetFile(dataFile)
    new PersistenceUri(ddf.getEngine, dataFile)
  }

  override def getDataFileName(): String = {
    this.getFolderPath(ddf.getNamespace, ddf.getName, "data")
  }

  override def getSchemaFileName(): String = {
    this.getFolderPath(ddf.getNamespace, ddf.getName, "schema")
  }

  def getFolderPath(namespace: String, name: String, subfolder: String) = {
    val directory = this.locateOrCreatePersistenceSubdirectory(namespace)
    s"$directory/$name/$subfolder"
  }
  override def load(namespace: String, name: String): IPersistible = {
    val schemaPath = this.getFolderPath(namespace, name, "schema")
    val dataPath = this.getFolderPath(namespace, name, "data")
    val manager = this.getManager
    val ctx = manager.asInstanceOf[SparkDDFManager].getHiveContext
    val schemaRDD = ctx.parquetFile(dataPath)
    val schema = JsonSerDes.loadFromFile(schemaPath).asInstanceOf[Schema]

    val ddf = manager.newDDF(manager, schemaRDD, Array(classOf[SchemaRDD]), manager.getNamespace, null, schema)
    ddf
  }
}
