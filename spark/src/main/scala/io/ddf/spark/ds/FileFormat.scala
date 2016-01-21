package io.ddf.spark.ds

import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnType
import io.ddf.exception.DDFException
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._


trait FileFormat

case class CsvFileFormat(schema: Option[StructType]) extends FileFormat

case class JsonFileFormat(flatten: Boolean) extends FileFormat

case class ParquetFileFormat(flatten: Boolean) extends FileFormat

object FileFormat {

  /**
    * Check options for the current file format to use, based on "format" option.
    *
    * @param options
    * @return a file format from options
    */
  def apply(options: Map[AnyRef, AnyRef]): FileFormat = {
    val formatType = options.get("format")
    if (formatType.isEmpty) throw new DDFException("Missing format option")
    formatType.get.toString.trim.toLowerCase match {
      case "csv" => CsvFileFormat(options)
      case "json" => JsonFileFormat(options)
      case "parquet" => ParquetFileFormat(options)
      case _ => throw new DDFException(s"Unknown file format $formatType")
    }
  }
}

object CsvFileFormat {

  def toSparkType(typ: ColumnType): DataType = {
    typ match {
      case ColumnType.TINYINT => ByteType
      case ColumnType.SMALLINT => ShortType
      case ColumnType.INT => IntegerType
      case ColumnType.BIGINT => LongType
      case ColumnType.FLOAT => FloatType
      case ColumnType.DOUBLE => DoubleType
      case ColumnType.DECIMAL => DecimalType()
      case ColumnType.STRING => StringType
      case ColumnType.BOOLEAN => BooleanType
      case ColumnType.TIMESTAMP => TimestampType
      case ColumnType.DATE => DateType
      case _ =>
        throw new DDFException(s"csv data cannot contains type $typ")
    }
  }

  def toSparkStructType(schema: Schema): StructType = {
    val fields = schema.getColumns.map { col =>
      StructField(col.getName, toSparkType(col.getType), nullable = true)
    }
    StructType(fields)
  }

  def apply(options: Map[AnyRef, AnyRef]): CsvFileFormat = {
    if (!options.containsKey("schema")) {
      throw new DDFException("schema param is required for csv format")
    }
    val schema = options.get("schema") map { s => toSparkStructType(new Schema(s.toString)) }
    new CsvFileFormat(schema)
  }
}

object JsonFileFormat {
  def apply(options: Map[AnyRef, AnyRef]): JsonFileFormat = {
    val flatten = options.getOrElse("flatten", false).toString == "true"
    new JsonFileFormat(flatten)
  }
}

object ParquetFileFormat {
  def apply(options: Map[AnyRef, AnyRef]): ParquetFileFormat = {
    val flatten = options.getOrElse("flatten", false).toString == "true"
    new ParquetFileFormat(flatten)
  }
}
