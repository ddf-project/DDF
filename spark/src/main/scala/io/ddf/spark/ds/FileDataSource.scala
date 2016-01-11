package io.ddf.spark.ds

import java.util
import java.util.UUID

import io.ddf.content.Schema
import io.ddf.ds.{BaseDataSource, User, UsernamePasswordCredential}
import io.ddf.exception.{DDFException, UnauthenticatedDataSourceException}
import io.ddf.spark.SparkDDFManager
import io.ddf.spark.util.SparkUtils
import io.ddf.{DDF, DDFManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.util.{Success, Try}


/**
  * Data source to load files from local or hdfs file system
  *
  * @param uri URI of the data source,
  *            should be"file://" for local file system and "hdfs://namenode" for HDFS
  * @param manager the DDF manager for this source
  */
class FileDataSource(uri: String, manager: DDFManager) extends BaseDataSource(uri, manager) {

  override def loadDDF(user: User, options: util.Map[AnyRef, AnyRef]): DDF = {
    val dataUri = getDataFilesUri(user, options)
    val format = FileFormat(options)
    val ddf = format match {
      case format: CsvFileFormat =>
        val table = createTable(dataUri, format)
        manager.sql2ddf(s"select * from $table", "spark")
      case format: JsonFileFormat =>
        val ddf = sparkRead(dataUri) { (ctx, uri) => ctx.read.json(uri) }
        if (format.flatten) ddf.getFlattenedDDF else ddf
      case format: ParquetFileFormat =>
        val ddf = sparkRead(dataUri) { (ctx, uri) => ctx.read.parquet(uri) }
        if (format.flatten) ddf.getFlattenedDDF else ddf
      case _ =>
        throw new DDFException(s"Unsupported file format: $format")
    }

    // try to set columns name if a schema option is passed
    if (options.containsKey("schema")) {
      val maybeSchema = Try {new Schema(options.get("schema").toString)}
      maybeSchema match {
        case Success(schema) =>
          ddf.setColumnNames(schema.getColumnNames)
        case _ => // do nothing
      }
    }

    ddf
  }

  /**
    * Read files from given URI using Spark SQL context.
    *
    * @param fileUri the URI of files to load
    * @param read a function that take a Spark's HiveContext and an Uri and return a DataFrame
    * @return a new DDF from source files
    */
  protected def sparkRead(fileUri: String)(read: (HiveContext, String) => DataFrame): DDF = {
    if (!manager.isInstanceOf[SparkDDFManager]) {
      throw new DDFException(s"Loading of $fileUri is only supported with SparkDDFManager")
    }
    val context = manager.asInstanceOf[SparkDDFManager].getHiveContext
    val df = read(context, fileUri)
    SparkUtils.df2ddf(df, manager)
  }

  /**
    * Build the full URI for source files to load.
    *
    * An option named "path" will be used as the path to source files.
    * It will be appended after the data source URI to become URI of source files.
    *
    * @param user the current user
    * @param options loading options
    * @return full URI of source files
    */
  protected def getDataFilesUri(user: User, options: util.Map[AnyRef, AnyRef]): String = {
    val path = options.getOrElse("path", "").toString
    if (uri == "hdfs://" || uri == "file://") {
      // special uri for local hdfs
      path
    } else {
      s"$uri$path"
    }
  }

  protected def createTable(dataUri: String, format: CsvFileFormat): String = {
    val tableName = UUID.randomUUID().toString.replace("-", "_")
    val sqlCmd =
      s"""CREATE EXTERNAL TABLE $tableName (${format.schema})
          |   ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
          |     WITH serdeproperties ('separatorChar' = '${format.delimiter}', 'quoteChar' = '${format.quote}')
          | STORED AS TEXTFILE LOCATION '$dataUri'""".stripMargin.replaceAll("\n", " ")
    manager.sql(sqlCmd, "spark")
    tableName
  }
}

/**
  * DataSource to load files from S3
  *
  * @param uri URI of the S3 source, in s3://<bucket-name> format
  * @param manager the DDF Manager for this source
  */
class S3DataSource(uri: String, manager: DDFManager) extends FileDataSource(uri, manager) {

  override def getDataFilesUri(user: User, options: util.Map[AnyRef, AnyRef]): String = {
    uri match {
      case S3DataSource.URI_PATTERN(bucket) =>
        val path = options.getOrElse("path", "").toString
        val absolutePath = if (path.startsWith("/")) path else s"/$path"
        val credential = Option(user.getCredential(uri))
        credential match {
          case Some(credential: UsernamePasswordCredential) =>
            val username = credential.getUsername
            val password = credential.getPassword
            s"s3n://$username:$password@$bucket$absolutePath"
          case Some(cred) =>
            throw new UnauthenticatedDataSourceException(s"Incompatible credential for S3 source: $cred")
          case None =>
            throw new UnauthenticatedDataSourceException()
        }
      case _ =>
        // should not happen
        throw new DDFException(s"Invalid S3 source uri: $uri")
    }
  }

}

object S3DataSource {
  val URI_PATTERN = "s3n?://([^/]*)".r
}
