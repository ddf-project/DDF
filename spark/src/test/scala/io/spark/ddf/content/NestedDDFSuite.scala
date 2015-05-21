package io.spark.ddf.content



import io.spark.ddf.ATestSuite
import io.spark.ddf.util.SparkUtils
import org.apache.spark.sql.DataFrame
import io.ddf.DDF
import scala.collection.JavaConverters._
/**
  */
class NestedDDFSuite extends ATestSuite {

  /*
  test("test JSON with primitiveFieldAndType") {
    val df:DataFrame = jsonRDD(primitiveFieldAndType)
    df.printSchema()
    println("Number of records: " + df.count())
  }*/


  //TODO add assertion to really have unit tests
  test("get flattened columns from struct dataframe loaded form a JSON file") {
    println("\n\n ================================= get flatten columns from struct dataframe")
    val path = "../resources/test/sleep_data_sample.json"
    val sqlCtx = manager.getHiveContext
    val df:DataFrame = sqlCtx.jsonFile(path)

    println("<<<< dataframe created from json file at " + path)
    df.printSchema()
    println("Number of records: " + df.count())

    println("<<<< all flattened cols from the dataframe:")
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
    val sqlCtx = manager.getHiveContext
    val df:DataFrame = sqlCtx.jsonFile(path)

    val ddf: DDF = manager.newDDF(manager, df, Array(classOf[DataFrame]), null, null, SparkUtils.schemaFromDataFrame(df))
    println("ddf schema: \n" + ddf.getSchema.toString)

    println("<<<< Get flattened DDF")
    val fddf: DDF = ddf.getFlattenedDDF(Array("uid", "data"))
    println("flattened_ddf schema: \n" + fddf.getSchema.toString)
    println("<<<<< sample 10 elements from the flattenedDDF")
    val sample = fddf.VIEWS.head(10)
    for(x <- sample.asScala.toList)
      println(x)
  }


  /* TODO fix bugs
  test("get full flatten DDF loaded from a JSON file") {
    println("\n\n ================================= get full flatten DDF ")
    val path = "../resources/test/sleep_data_sample.json"
    val sqlCtx = manager.getHiveContext
    val df:DataFrame = sqlCtx.jsonFile(path)

    q = s"""
    select _id.oid as id.oid,cid,created_at.date,data.bookmarkTime,data.isFirstSleepOfDay,
      data.normalizedSleepQuality,data.realDeepSleepTimeInMinutes,data.realEndTime,data.realSleepTimeInMinutes,
      data.realStartTime,data.sleepStateChanges,itype,ts,u_at.date,uid.oid
    from
    """
    val ddf: DDF = manager.newDDF(manager, df, Array(classOf[DataFrame]), null, null, SparkUtils.schemaFromDataFrame(df))
    println("ddf schema: \n" + ddf.getSchema.toString)

    println("<<<< Get flattened DDF")
    val fddf: DDF = ddf.getFlattenedDDF()
    println("flattened_ddf schema: \n" + fddf.getSchema.toString)
    println("<<<<< sample 10 elements from the flattenedDDF")
    val sample = fddf.VIEWS.head(10)
    for(x <- sample.asScala.toList)
      println(x)

    println("<<<<< query 10 elements from the flattenedDDF")
    val qdata = fddf.sql2ddf(s"select * from ${fddf.getTableName} limit 10")
    for(x <- qdata.VIEWS.head(10).asScala.toList)
      println(x)

  }
  */
}
