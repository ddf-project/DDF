package io.spark.ddf.content

import io.ddf.DDF
import io.spark.ddf.ATestSuite
import io.spark.ddf.util.SparkUtils
import org.apache.spark.sql.DataFrame
import org.junit.Assert

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
    val lddf = ddf.sql2txt("select * from @this limit 3", "")
    lddf.asScala.toList.foreach(println)

    val fddf: DDF = ddf.getFlattenedDDF()
    println("---flattened_ddf schema: \n" + fddf.getSchema.getColumnNames)

    println("---flattened_ddf values:")
    val lfddf = fddf.sql2txt("select * from @this limit 3", "")
    lfddf.asScala.toList.foreach(println)
  }

  test("test some stats functions on flattened DDF") {
    println("\n\n ================================= test some stats functions on flattened DDF")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf = json2ddf(path)
    val fddf: DDF = ddf.getFlattenedDDF()
    val qdf = fddf.sql2ddf(s"select data_realDeepSleepTimeInMinutes from ${fddf.getTableName}")

    val expectedArray = Array(122.0,183.5,241.75)
    println("get FiveNum")
    val fn = qdf.getFiveNumSummary
    Assert.assertEquals(expectedArray(0), fn(0).getFirstQuantile)
    Assert.assertEquals(expectedArray(1), fn(0).getMedian)
    Assert.assertEquals(expectedArray(2), fn(0).getThirdQuantile)
    fn.foreach(x => {println(Array(x.getFirstQuantile, x.getMedian, x.getThirdQuantile).mkString(","))})

    println("get vectorQuantiles")
    val vq = qdf.getVectorQuantiles(Array(0.25, 0.5, 0.75))
    Assert.assertEquals(expectedArray(0), vq(0))
    println(vq.mkString(","))

    println("get Summary")
    val sum = qdf.getSummary
    sum.foreach(x => {println(x.toString)})
  }



  def json2ddf(path:String): DDF = {
    val sqlCtx = manager.getHiveContext
    val jdf: DataFrame = sqlCtx.jsonFile(path)
    val df = SparkUtils.getDataFrameWithValidColnames(jdf)
    manager.newDDF(manager, df, Array(classOf[DataFrame]), null, null, SparkUtils.schemaFromDataFrame(df))
  }

}
