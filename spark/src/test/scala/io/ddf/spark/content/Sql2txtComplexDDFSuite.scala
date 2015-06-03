package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.spark.ATestSuite
import io.ddf.spark.util.SparkUtils
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/**
  */
class Sql2txtComplexDDFSuite extends ATestSuite {


  test("test printout value of complex type dataframe using json") {
    println("\n\n ================================= test printout value of complex type dataframe using json")
    val path = "../resources/test/sleep_data_sample.json"
    val sqlCtx = manager.getHiveContext
    val df: DataFrame = sqlCtx.jsonFile(path).sample(false, 0.1)
    df.printSchema()

    println("--- sample dataframe with all columns:")
    val strLst = SparkUtils.jsonForComplexType(df, "\t")
    strLst.foreach(println)

    println("--- sample dataframe with column data.sleepStateChanges only:")
    val strLst1 = SparkUtils.jsonForComplexType(df.select("data.sleepStateChanges"), "\t")
    strLst1.foreach(println)

  }

  test("test sql2txt with json for complex type DDF") {
    println("\n\n ================================= test sql2txt with json for complex type DDF")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf = json2ddf(path)
    println("---ddf schema: \n" + ddf.getSchema.getColumnNames)

    println("---ddf values:")
    val lddf = ddf.sql2txt("select * from @this limit 10", "")
    lddf.asScala.toList.foreach(println)

    val fddf: DDF = ddf.getFlattenedDDF()
    println("---flattened_ddf schema: \n" + fddf.getSchema.getColumnNames)

    println("---flattened_ddf values:")
    val lfddf = fddf.sql2txt("select * from @this limit 10", "")
    lfddf.asScala.toList.foreach(println)
  }

  def json2ddf(path:String): DDF = {
    val sqlCtx = manager.getHiveContext
    val jdf: DataFrame = sqlCtx.jsonFile(path)
    val df = SparkUtils.getDataFrameWithValidColnames(jdf)
    manager.newDDF(manager, df, Array(classOf[DataFrame]), null, null, SparkUtils.schemaFromDataFrame(df))
  }

}
