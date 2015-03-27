package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import io.spark.ddf.{SparkDDFManager, SparkDDF}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.SchemaRDD
import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.sql.catalyst.types.StructType
import scala.collection.JavaConversions._
import org.apache.spark.sql.execution.{SparkPlan, SparkLogicalPlan, ExistingRdd}
import org.apache.spark.sql.catalyst.types._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.AttributeReference

/**
  */
class ArrayObject2SchemaRDD(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: RDD[Array[Object]] => {
        val rddRow = rdd.map {
          row => Row(row: _*)
        }
        val schema = StructType(ddf.getSchemaHandler.getColumns.map(col => ArrayObject2SchemaRDD.column2StructField(col)))
        val schemaRDD = ddf.getManager.asInstanceOf[SparkDDFManager].getHiveContext.applySchema(rddRow, schema)
        new Representation(schemaRDD, RepresentationHandler.SCHEMARDD.getTypeSpecsString)
      }
    }
  }
}

object ArrayObject2SchemaRDD {
  def column2StructField(column: Column): StructField = {
    column.getType match {
      case ColumnType.DOUBLE => StructField(column.getName, DoubleType, true)
      case ColumnType.INT => StructField(column.getName, IntegerType, true)
      case ColumnType.STRING => StructField(column.getName, StringType, true)
      case ColumnType.FLOAT => StructField(column.getName, FloatType, true)
      case ColumnType.BIGINT => StructField(column.getName, LongType, true)
      case ColumnType.LONG => StructField(column.getName, LongType, true)
      case ColumnType.LOGICAL => StructField(column.getName, BooleanType, true)
      case ColumnType.TIMESTAMP => StructField(column.getName, TimestampType, true)
    }
  }
}
