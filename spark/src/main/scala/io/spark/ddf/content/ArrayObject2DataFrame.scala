package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import io.spark.ddf.{SparkDDFManager, SparkDDF}
import org.apache.spark.sql.Row
import io.ddf.content.Schema.{ColumnType, Column}
import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

/**
  */
class ArrayObject2DataFrame(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: RDD[Array[Object]] => {
        val rddRow = rdd.map {
          row => Row(row: _*)
        }
        val schema = StructType(ddf.getSchemaHandler.getColumns.map(col => ArrayObject2DataFrame.column2StructField(col)))
        val dataFrame = ddf.getManager.asInstanceOf[SparkDDFManager].getHiveContext.applySchema(rddRow, schema)
        new Representation(dataFrame, RepresentationHandler.DATAFRAME.getTypeSpecsString)
      }
    }
  }
}

// TODO review this @huan @freeman
object ArrayObject2DataFrame {
  def column2StructField(column: Column): StructField = {
    column.getType match {
      case ColumnType.TINYINT => StructField(column.getName, ByteType, true)
      case ColumnType.SMALLINT => StructField(column.getName, ShortType, true)
      case ColumnType.INT => StructField(column.getName, IntegerType, true)
      case ColumnType.BIGINT => StructField(column.getName, LongType, true)
      case ColumnType.FLOAT => StructField(column.getName, FloatType, true)
      case ColumnType.DOUBLE => StructField(column.getName, DoubleType, true)
      case ColumnType.DECIMAL => StructField(column.getName, DecimalType(), true)
      case ColumnType.STRING => StructField(column.getName, StringType, true)
      case ColumnType.BOOLEAN => StructField(column.getName, BooleanType, true)
      case ColumnType.TIMESTAMP => StructField(column.getName, TimestampType, true)
      case ColumnType.BINARY => StructField(column.getName, BinaryType, true)
      case ColumnType.TIMESTAMP => StructField(column.getName, TimestampType, true)
      case ColumnType.DATE => StructField(column.getName, DateType, true)
      case ColumnType.STRUCT => StructField(column.getName, StructType(_), true)
      case ColumnType.ARRAY => StructField(column.getName, ArrayType(_,_), true)
      case ColumnType.MAP => StructField(column.getName, MapType(_,_,_), true)
    }
  }
}
