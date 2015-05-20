package io.spark.ddf.util

import java.util
import java.util.{Map => JMap}
import org.apache.spark.sql.types.{StructType, StructField}

import scala.collection.JavaConverters._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import io.ddf.content.Schema
import scala.collection.mutable.ArrayBuffer
import java.util.{List => JList}
import io.ddf.content.Schema.Column
import com.google.common.collect.Lists
import java.util.ArrayList
import scala.util
import io.ddf.exception.DDFException

import scala.util


/**
  */

object SparkUtils {
  /**
   * Create custom sharkContext with adatao's spark.kryo.registrator
   * @param master
   * @param jobName
   * @param sparkHome
   * @param jars
   * @param environment
   * @return
   */
  def createSparkConf(master: String, jobName: String, sparkHome: String, jars: Array[String],
                      environment: JMap[String, String]): SparkConf = {
    //val conf = SharkContext.createSparkConf(master, jobName, sparkHome, jars, environment.asScala)
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(jobName)
      .setJars(jars)
      .setExecutorEnv(environment.asScala.toSeq)
    conf.set("spark.kryo.registrator", System.getProperty("spark.kryo.registrator", "io.spark.content.KryoRegistrator"))
  }

  def createSparkContext(master: String, jobName: String, sparkHome: String, jars: Array[String],
                         environment: JMap[String, String]): SparkContext = {
    val conf = createSparkConf(master, jobName, sparkHome, jars, environment)
    new SparkContext(conf)
  }

  def schemaFromDataFrame(schemaRDD: DataFrame): Schema = {
    val schema = schemaRDD.schema
    //println("<<<< schema: " + schema)
    val cols: ArrayList[Column] = Lists.newArrayList();
    for(field <- schema.fields) {
      val colType = spark2DDFType(field.dataType.typeName)
      val colName = field.name
      cols.add(new Column(colName, colType))
    }
    new Schema(null, cols)
  }

  /**
   *
   * @param df the input dataframe
   * @param colNames subset of column that user wants to flatten
   * @return a list of names of non-struct fields flattened from the dataframe
   */
  def flattenColumnNamesFromDataFrame(df: DataFrame, colNames: Array[String]): Array[String] = {
    val result: ArrayBuffer[String] = new ArrayBuffer[String]()
    val schema = df.schema
    val fields =
      if(colNames == null || colNames.isEmpty) {
        schema.fields
      } else {
        val flds:ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
        for(name <- colNames) {
          if (schema.fieldNames.contains(name))
            flds.append(schema.apply(name))
          else
            throw new DDFException("Error: column-name " + name + " does not exist in the dataset")
        }
        flds.toArray
      }

    for(field <- fields) {
      result.appendAll(flattenColumnNamesFromStruct(field))
    }
    result.toArray[String]
  }

  def flattenColumnNamesFromDataFrame(df: DataFrame): Array[String] = {
    flattenColumnNamesFromDataFrame(df, null)
  }

  /**
   * @param structField
   * @return all primitive column paths inside the struct
   */
  private def flattenColumnNamesFromStruct(structField: StructField): Array[String] = {
    var result:ArrayBuffer[String] = new ArrayBuffer[String]()
    flattenColumnNamesFromStruct(structField, result, "")
    result.toArray[String]
  }

  private def flattenColumnNamesFromStruct(structField: StructField, resultList: ArrayBuffer[String], curColName: String): Unit = {
    val colName = if(curColName == "") structField.name else (curColName + "." + structField.name)
    val dType = structField.dataType

    if(dType.typeName != "struct") {
      resultList.append(colName)
    } else {
      val fields = dType.asInstanceOf[StructType].fields
      for(field <- fields) {
        flattenColumnNamesFromStruct(field, resultList, colName)
      }
    }

  }

  def spark2DDFType(colType: String): String = {
    //println(colType)
    colType match {
      case "integer" => "INT"
      case "string" => "STRING"
      case "float"  => "FLOAT"
      case "double" => "DOUBLE"
      case "timestamp" => "TIMESTAMP"
      case "long"     => "LONG"
      case "boolean"  => "BOOLEAN"
      case "struct" => "STRUCT"
      case "array" => "ARRAY"
      case x => throw new DDFException(s"Type not support $x")
    }
  }
}
