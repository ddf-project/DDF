package io.spark.ddf.util

import java.util.{Map => JMap}
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
    val cols: ArrayList[Column] = Lists.newArrayList();
    for(field <- schema.fields) {
      val colType = spark2DDFType(field.dataType.typeName)
      val colName = field.name
      cols.add(new Column(colName, colType))
    }
    new Schema(null, cols)
  }

  def spark2DDFType(colType: String): String = {
    colType match {
      case "integer" => "INT"
      case "string" => "STRING"
      case "float"  => "FLOAT"
      case "double" => "DOUBLE"
      case "timestamp" => "TIMESTAMP"
      case "long"     => "LONG"
      case "boolean"  => "BOOLEAN"
      case "struct" => "STRUCT"
      case x => throw new DDFException(s"Type not support $x")
    }
  }
}
