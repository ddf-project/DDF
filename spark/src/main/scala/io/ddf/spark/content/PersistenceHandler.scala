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
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path

/**
 * author: daoduchuan
 */

class PersistenceHandler(ddf: DDF) extends BPersistenceHandler(ddf) {

  override def persist(doOverwrite: Boolean): PersistenceUri = {
    val dataFile = this.getDataFileName()
    val schemaFile = this.getSchemaFileName()
    if(doOverwrite) {
      mLog.info(">>>> overwriting")
      if(Utils.fileExists(dataFile)) {
        mLog.info(s">>> file $dataFile exists deleting")
        Utils.deleteFile(dataFile)
      }
      if(Utils.fileExists(schemaFile)) {
        mLog.info(s">>> file $schemaFile exists deleting")
        Utils.deleteFile(schemaFile)
      }
    }

    val folderPath = this.getFolderPath(ddf.getNamespace, ddf.getName, "")
    Utils.writeToFile(schemaFile, JsonSerDes.serialize(this.getDDF.getSchema) + "\n")
    val schemaRDD = ddf.getRepresentationHandler.get(classOf[SchemaRDD]).asInstanceOf[SchemaRDD]
    schemaRDD.saveAsParquetFile(dataFile)
    new PersistenceUri(ddf.getEngine, folderPath)
  }

  override def getDataFileName(): String = {
    this.getFolderPath(ddf.getNamespace, ddf.getName, "data")
  }

  override def getSchemaFileName(): String = {
    this.getFolderPath(ddf.getNamespace, ddf.getName, "schema")
  }

  def getFolderPath(namespace: String, name: String, subfolder: String) = {
    val directory = this.locateOrCreatePersistenceSubdirectory(namespace)
    if(subfolder != null || !subfolder.isEmpty) {
      s"$directory/$name/$subfolder"
    } else {
      s"$directory/$name"
    }
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

  def listPersistedDDFUris(): List[String] = {
    val persistenceDirectory = this.locateOrCreatePersistenceSubdirectory(this.ddf.getNamespace)
    val listDDFs = Utils.listHDFSSubDirectory(persistenceDirectory).map{
      directory => new Path(directory).getName
    }
    listDDFs.map {
      ddfName => {
        val folderPath = this.getFolderPath(ddf.getNamespace, ddfName, "")
        new PersistenceUri(ddf.getEngine, folderPath).toString
      }
    }.toList
  }
}
