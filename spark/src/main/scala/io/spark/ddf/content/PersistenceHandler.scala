package io.spark.ddf.content

import io.basic.ddf.content.{PersistenceHandler => BPersistenceHandler}
import io.ddf.DDF
import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.util.Utils
import io.ddf.util.Utils.JsonSerDes
import org.apache.spark.sql.SchemaRDD

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
    val nameSpace = ddf.getNamespace
    val directory = this.locateOrCreatePersistenceSubdirectory(nameSpace)
    val ddfName = this.getDDF.getName
    s"$directory/$ddfName/data"
  }

  override def getSchemaFileName(): String = {
    val nameSpace = ddf.getNamespace
    val directory = this.locateOrCreatePersistenceSubdirectory(nameSpace)
    val ddfName = this.getDDF.getName
    s"$directory/$ddfName/schema"
  }
}
