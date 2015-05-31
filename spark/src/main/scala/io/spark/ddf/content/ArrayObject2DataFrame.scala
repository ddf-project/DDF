package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import io.spark.ddf.{SparkDDFManager, SparkDDF}
import org.apache.spark.sql.Row
import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.sql.types.StructType
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

object ArrayObject2DataFrame {
  def column2StructField(column: Column): StructField = {
    column.getType match {
      case ColumnType.DOUBLE => StructField(column.getName, DoubleType, true)
      case ColumnType.INT => StructField(column.getName, IntegerType, true)
      case ColumnType.STRING => StructField(column.getName, StringType, true)
      case ColumnType.FLOAT => StructField(column.getName, FloatType, true)
      case ColumnType.BIGINT => StructField(column.getName, LongType, true)
      case ColumnType.LONG => StructField(column.getName, LongType, true)
      case ColumnType.BOOLEAN => StructField(column.getName, BooleanType, true)
      case ColumnType.TIMESTAMP => StructField(column.getName, TimestampType, true)
    }
  }
}
