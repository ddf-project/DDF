package io.ddf.spark.ds

import java.util

import io.ddf.content.Schema
import io.ddf.exception.DDFException

import scala.collection.JavaConversions._

trait FileFormat

case class CsvFileFormat(schema: Schema, delimiter: String, quote: String) extends FileFormat

case class JsonFileFormat(flatten: Boolean) extends FileFormat

case class ParquetFileFormat(flatten: Boolean) extends FileFormat

object FileFormat {

  /**
    * Check options for the current file format to use, based on "format" option.
    *
    * @param options
    * @return a file format from options
    */
  def apply(options: util.Map[AnyRef, AnyRef]): FileFormat = {
    val formatType = options.get("format")
    if (formatType == null) throw new DDFException("Missing format option")
    formatType.toString.toLowerCase match {
      case "csv" => CsvFileFormat(options)
      case "json" => JsonFileFormat(options)
      case "parquet" => ParquetFileFormat(options)
      case _ => throw new DDFException(s"Unknown file format $formatType")
    }
  }
}

object CsvFileFormat {
  def apply(options: util.Map[AnyRef, AnyRef]): CsvFileFormat = {
    if (!options.containsKey("schema")) {
      throw new DDFException("CSV file format requires a schema option")
    }
    val schema = new Schema(options.get("schema").toString)
    val delimiter = options.getOrElse("delimiter", ",").toString
    val quote = options.getOrElse("quote", "\"").toString
    new CsvFileFormat(schema, delimiter, quote)
  }
}

object JsonFileFormat {
  def apply(options: util.Map[AnyRef, AnyRef]): JsonFileFormat = {
    val flatten = options.getOrElse("flatten", false).toString == "true"
    new JsonFileFormat(flatten)
  }
}

object ParquetFileFormat {
  def apply(options: util.Map[AnyRef, AnyRef]): ParquetFileFormat = {
    val flatten = options.getOrElse("flatten", false).toString == "true"
    new ParquetFileFormat(flatten)
  }
}
