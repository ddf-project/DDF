package io.ddf.spark.util

import java.io.CharArrayWriter
import java.util
import java.util.{Map => JMap}
import com.fasterxml.jackson.core.{JsonGenerator, JsonFactory}
import com.google.common.base.Strings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column => DFColumn}
import io.ddf.content.Schema
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import java.util.{List => JList}
import io.ddf.content.Schema.Column
import com.google.common.collect.Lists
import java.util.ArrayList
import io.ddf.exception.DDFException
import scala.collection.JavaConverters._

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
    conf.set("spark.kryo.registrator", System.getProperty("spark.kryo.registrator", "io.ddf.spark.content.KryoRegistrator"))
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
      val colType = spark2DDFType(field.dataType)
      val colName = field.name.trim
      cols.add(new Column(colName, colType))
    }
    new Schema(null, cols)
  }


  def str2SparkSchema(schema: String): StructType = {
    StructType(
      schema.split(",").map(
        attr => {
          val nameAndType = attr.trim.split(" ")
          StructField(nameAndType(0), str2SparkType(nameAndType(1)), true )
        }

      )
    )
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
    val colName = if(curColName == "") structField.name else (curColName + "->" + structField.name)
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

  /**
   *
   * @param df
   * @param sep the separator to separate adjacent column
   * @return an Array of string showing the dataframe with complex column-object replaced by json string
   */
  def df2txt(df: DataFrame, sep: String): Array[String] = {
    val schema = df.schema
    //val df1: RDD[String] = df.map(r => rowToJSON(schema, r, sep)) // run in parallel
    //df1.collect()
    df.collect().map(r => row2txt(schema, r, sep)) // run sequentially
  }

  /**
   *
   * @param rowSchema
   * @param row
   * @param separator
   * @return
   */
  def row2txt(rowSchema: StructType, row: Row, separator: String): String = {
    val writer = new CharArrayWriter()
    val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    val complexTypes = Array("ArrayType", "StructType", "MapType")
    var i = 0
    rowSchema.zip(row.toSeq).foreach {
      case (field, v) =>
        if(i > 0)
          gen.writeRaw(separator)
        i = i+1
        val simpleTypeString: String = field.dataType.simpleString
        if(v == null)
          gen.writeNull()
        else if(!complexTypes.contains(simpleTypeString))
          gen.writeRaw(v.toString.replaceAll("\t", "\\\\t"))
        else
          data2json(field.dataType, v, gen)
    }
    gen.close()
    writer.toString
  }

  /**
   * get value of a cell as string
   *
   * @param dataType
   * @param data
   * @return
   */
  def cell2txt(dataType: DataType, data: Any): String = {
    val writer = new CharArrayWriter()
    val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    val simpleTypeString: String = dataType.simpleString
    val complexTypes = Array("ArrayType", "StructType", "MapType")
    if(data == null)
      gen.writeNull()
    else if(!complexTypes.contains(simpleTypeString))
      gen.writeRaw(data.toString)
    else
      data2json(dataType, data, gen)

    gen.close()
    writer.toString
  }

  /**
   * A recursive function to output json string for a SparkSQL data object
   *
   * @param dataType data-type of the data
   * @param data the data object
   * @param gen JsonGenerator that write value in appropriate format
   */
  private def data2json(dataType: DataType, data: Any, gen: JsonGenerator, isFirst: Boolean = false): Unit = { 
    if(isFirst)
      gen.flush()
    
    (dataType,data) match {
      case (_, null) | (NullType, _) => gen.writeNull()
      case (StringType, v: String) => gen.writeString(v.toString.replaceAll("\t", "\\\\t"))
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: java.math.BigDecimal) => gen.writeNumber(v)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => data2json(udt.sqlType, v, gen)

      case (ArrayType(ty, _), v: Seq[_]) =>
        gen.writeStartArray()
        v.foreach(data2json(ty, _, gen))
        gen.writeEndArray()

      case (MapType(kv, vv, _), v: Map[_, _]) =>
        gen.writeStartObject()
        v.foreach { p =>
          gen.writeFieldName(p._1.toString)
          data2json(vv, p._2, gen)
        }
        gen.writeEndObject()

      case (StructType(ty), v: Row) =>
        gen.writeStartObject()
        ty.zip(v.toSeq).foreach {
          case (_, null) =>
          case (field, v) =>
            gen.writeFieldName(field.name)
            data2json(field.dataType, v, gen)
        }
        gen.writeEndObject()
    }
  }

  def getDataFrameWithValidColnames(df: DataFrame): DataFrame = {
    // remove '_' if '_' is at the start of a col name
    val colNames = df.columns.map { colName =>
      if (colName.charAt(0) == '_') new DFColumn(colName).as(colName.substring(1)) else new DFColumn(colName)
    }
    df.select(colNames :_*)
  }



  def spark2DDFType(colType: DataType): Schema.ColumnType = {
    //println(colType)
    colType match {
      case ByteType => Schema.ColumnType.TINYINT
      case ShortType => Schema.ColumnType.SMALLINT
      case IntegerType => Schema.ColumnType.INT
      case LongType     => Schema.ColumnType.BIGINT
      case FloatType  => Schema.ColumnType.FLOAT
      case DoubleType => Schema.ColumnType.DOUBLE
      case DecimalType() => Schema.ColumnType.DECIMAL
      case StringType => Schema.ColumnType.STRING
      case BooleanType  => Schema.ColumnType.BOOLEAN
      case BinaryType => Schema.ColumnType.BINARY
      case TimestampType => Schema.ColumnType.TIMESTAMP
      case DateType => Schema.ColumnType.DATE
      case StructType(_) => Schema.ColumnType.STRUCT
      case ArrayType(_, _) => Schema.ColumnType.ARRAY
      case MapType(_, _, _) => Schema.ColumnType.MAP
      case x => throw new DDFException(s"Type not support $x")
    }
  }

  def str2SparkType(str: String): DataType = {
    // TODO, add more type here
    str.toLowerCase match {
      case "string" => StringType
      case "int" => IntegerType
      case "long" => LongType
      case "double" => DoubleType
      case "float" => FloatType
      case "timestamp" => TimestampType
      case "datetype" => DateType
      case x => throw new DDFException(s"Type not support $x")
    }
  }
}
