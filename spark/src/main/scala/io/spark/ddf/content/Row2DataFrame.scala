package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConversions._
import io.spark.ddf.SparkDDFManager

/**
  */
class Row2DataFrame(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: RDD[Row] => {
        val schema = StructType(ddf.getSchemaHandler.getColumns.map(col => ArrayObject2DataFrame.column2StructField(col)))
        val schemaRDD = ddf.getManager.asInstanceOf[SparkDDFManager].getHiveContext.applySchema(rdd, schema)
        new Representation(schemaRDD, RepresentationHandler.DATAFRAME.getTypeSpecsString)
      }
    }
  }
}
