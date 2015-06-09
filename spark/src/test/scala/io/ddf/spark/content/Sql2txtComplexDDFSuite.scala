package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.spark.ATestSuite
import io.ddf.spark.util.SparkUtils
import org.apache.spark.sql.DataFrame
import org.junit.Assert

import scala.collection.JavaConverters._

/**
  */
class Sql2txtComplexDDFSuite extends ATestSuite {

  /*
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
  */

  test("test sql2txt with json for complex type DDF") {
    println("\n\n ================================= test sql2txt with json for complex type DDF")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf = json2ddf(path)
    val ddf_cols = ddf.getSchema.getColumnNames.toString
    println("---ddf schema: \n" + ddf_cols)
    Assert.assertEquals("[id, cid, created_at, data, itype, ts, u_at, uid]", ddf_cols)

    println("---ddf values:")
    val lddf = ddf.sql2txt("select * from @this limit 3", "")
    lddf.asScala.toList.foreach(println)

    val fddf: DDF = ddf.getFlattenedDDF()
    val fddf_cols = fddf.getSchema.getColumnNames.toString
    println("---flattened_ddf schema: \n" + fddf_cols)
    Assert.assertEquals("[id_oid, cid, created_at_date, data_bookmarkTime, data_isFirstSleepOfDay, data_normalizedSleepQuality, data_realDeepSleepTimeInMinutes, data_realEndTime, data_realSleepTimeInMinutes, data_realStartTime, data_sleepStateChanges, itype, ts, u_at_date, uid_oid]", fddf_cols)

    println("---flattened_ddf values:")
    val lfddf = fddf.sql2txt("select * from @this limit 3", "")
    lfddf.asScala.toList.foreach(println)

    println("---sample from flattened_ddf:")
    val sample = ddf.VIEWS.getRandomSample(0.1, false, 1)
    println("sample: ")
    //sample.getSchema.getColumns.asScala.toList.foreach(c => {println(c.getName + " - " + c.getType)})
    sample.VIEWS.head(3).asScala.toList.foreach(println)
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
    fn.foreach(x => {println(Array(x.getFirstQuantile, x.getMedian, x.getThirdQuantile).mkString(","))})
    Assert.assertEquals(expectedArray(0), fn(0).getFirstQuantile, 0.01)
    Assert.assertEquals(expectedArray(1), fn(0).getMedian, 0.01)
    Assert.assertEquals(expectedArray(2), fn(0).getThirdQuantile, 0.01)

    println("get vectorQuantiles")
    val vq = qdf.getVectorQuantiles(Array(0.25, 0.5, 0.75))
    println(vq.mkString(","))
    Assert.assertEquals(expectedArray(0), vq(0), 0.01)

    println("get Summary")
    val sum = qdf.getSummary
    sum.foreach(x => {println(x.toString)})
    Assert.assertEquals(189.59, sum(0).mean(), 0.01)
    Assert.assertEquals(85.5, sum(0).stdev(), 0.01)
    Assert.assertEquals(100, sum(0).count(), 0.01)
  }



  def json2ddf(path:String): DDF = {
    val sqlCtx = manager.getHiveContext
    val jdf: DataFrame = sqlCtx.jsonFile(path)
    val df = SparkUtils.getDataFrameWithValidColnames(jdf)
    manager.newDDF(manager, df, Array(classOf[DataFrame]), null, null, SparkUtils.schemaFromDataFrame(df))
  }

}
