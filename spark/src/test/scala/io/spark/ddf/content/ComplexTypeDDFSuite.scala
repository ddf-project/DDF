package io.spark.ddf.content



import io.spark.ddf.ATestSuite
import io.spark.ddf.util.SparkUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import io.ddf.DDF
import scala.collection.JavaConverters._
/**
  */
class ComplexTypeDDFSuite extends ATestSuite {


  /*
  test("query from spark dataframe loaded from JSON") {
    println("\n\n ================================= test spark dataframe loaded from JSON ")
    val path = "../resources/test/sleep_data_sample.json"
    val sqlCtx = manager.getHiveContext
    val df: DataFrame = sqlCtx.jsonFile(path)
    df.schema.fieldNames.foreach(println)
    df.printSchema

    val ndf = df.withColumnRenamed("_id", "id")
    ndf.schema.fieldNames.foreach(println)
    ndf.printSchema

    val a = Array("_id.oid","data", "data.bookmarkTime").map(new Column(_))
    val qdf1: DataFrame = df.select(a :_*)
    //val qdf1: DataFrame = df.select("_id.oid","data", "data.bookmarkTime")
    qdf1.printSchema
    qdf1.show

    ndf.registerTempTable("tmpTbl")
    val qdf2:DataFrame = sqlCtx.sql("select uid.oid as uid_oid, data, data.bookmarkTime, data.sleepStateChanges[0], data.sleepStateChanges[0][1] from tmpTbl")
    qdf2.printSchema
    qdf2.schema.fieldNames.foreach(println)
  }
*/

  // TODO add assertion to really have unit tests
  // currently, just printout for debugging
/*
  test("get flattened columns from struct dataframe loaded form a JSON file") {
    println("\n\n ================================= get flatten columns from struct dataframe")
    val path = "../resources/test/sleep_data_sample.json"
    val sqlCtx = manager.getHiveContext
    val df:DataFrame = sqlCtx.jsonFile(path)

    println("\n---- dataframe created from json file at " + path)
    df.printSchema()
    println("Number of records: " + df.count())

    println("\n---- all flattened cols from the dataframe:")
    val cols = SparkUtils.flattenColumnNamesFromDataFrame(df);
    for(col <- cols)
      println(col)


    println("<<<< cols flattened from cols '_id' and 'data' of the dataframe:")
    val selected_cols = SparkUtils.flattenColumnNamesFromDataFrame(df, Array("_id", "data"))
    for( col <- selected_cols)
      println(col)

  }

  test("get flatten DDF loaded from a JSON file") {
    println("\n\n ================================= get flatten DDF with selected columns ")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf: DDF = json2ddf(path)
    println("--- ddf schema: \n" + ddf.getSchema.getColumnNames)

    val fddf: DDF = ddf.getFlattenedDDF(Array("uid", "data"))
    println("---flattened_ddf schema: \n" + fddf.getSchema.getColumnNames)
    println("\n------ Sample 4 elements from the flattenedDDF")
    val sample = fddf.VIEWS.head(4)
    sample.asScala.toList.foreach(println)
  }

  test("test query result from flattened DDF with selected columns") {
    println("\n\n ================================= query result from flattened DDF with selected columns ")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf: DDF = json2ddf(path)
    println("---ddf schema: \n" + ddf.getSchema.getColumnNames)

    val fddf: DDF = ddf.getFlattenedDDF(Array("uid", "data")) // <--- only flatten columns 'uid' and 'data'
    println("---flattened_ddf schema: \n" + fddf.getSchema.getColumnNames)

    println("\n---- Query 4 elements from the flattenedDDF")
    val qdata = fddf.sql2ddf(s"select data_bookmarkTime from ${fddf.getTableName} limit 4")
    println("---query result from a flattened ddf: ")
    println("schema: " + qdata.getSchema.getColumnNames)
    println("data:")
    qdata.VIEWS.head(10).asScala.toList.foreach(println)
  }
*/
  /*
  test("test query result from flattened DDF with all columns") {
    println("\n\n ================================= query result from flattened DDF with all columns")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf = json2ddf(path)
    println("---ddf schema: \n" + ddf.getSchema.getColumnNames)
    ddf.VIEWS.head(3).asScala.toList.foreach(println)

    val fddf: DDF = ddf.getFlattenedDDF()
    println("---flattened_ddf schema: \n" + fddf.getSchema.getColumnNames)
    fddf.VIEWS.head(3).asScala.toList.foreach(println)

    println("\n---- Query 4 elements from the flattenedDDF")
    val qdata = fddf.sql2ddf(s"select data_bookmarkTime from ${fddf.getTableName} limit 4")
    println("---query result from a flattened ddf: ")
    println("schema: " + qdata.getSchema.getColumnNames)
    println("data:")
    qdata.VIEWS.head(3).asScala.toList.foreach(println)
  }

  */

  test("test some stats functions on flattened DDF") {
    println("\n\n ================================= test some stats functions on flattened DDF")
    val path = "../resources/test/sleep_data_sample.json"
    val ddf = json2ddf(path)
    val fddf: DDF = ddf.getFlattenedDDF()

    println("\nquery min, max, histogram from the flattenedDDF")
    val qdata = fddf.sql2txt(s"select min(data_realDeepSleepTimeInMinutes), max(data_realDeepSleepTimeInMinutes), avg(data_realDeepSleepTimeInMinutes), PERCENTILE(data_realDeepSleepTimeInMinutes, array(0, 1, 0.25, 0.5, 0.75)), histogram_numeric(data_realDeepSleepTimeInMinutes, 10) from ${fddf.getTableName}", "")
    qdata.get(0).split("\t").foreach(println)

    val qdf = fddf.sql2ddf(s"select data_realDeepSleepTimeInMinutes from ${fddf.getTableName}")
    println("get FiveNum")
    qdf.getFiveNumSummary.foreach(x => {
      println(Array(x.getFirstQuantile, x.getMedian, x.getThirdQuantile).mkString(","))
    })

    println("get vectorQuantiles")
    println(qdf.getVectorQuantiles(Array(0.25, 0.5, 0.75)).mkString(","))

    println("get Summary")
    qdf.getSummary.foreach(x => {
      println(x.toString)
    })
  }


  def json2ddf(path:String): DDF = {
    val sqlCtx = manager.getHiveContext
    val jdf: DataFrame = sqlCtx.jsonFile(path)
    val df = SparkUtils.getDataFrameWithValidColnames(jdf)
    manager.newDDF(manager, df, Array(classOf[DataFrame]), null, null, SparkUtils.schemaFromDataFrame(df))
  }

}
