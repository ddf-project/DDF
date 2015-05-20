package io.spark.ddf.content



import io.spark.ddf.ATestSuite
import io.spark.ddf.util.SparkUtils
import java.util.ArrayList
import org.apache.spark.sql.{SQLContext, DataFrame}
import scala.collection.JavaConverters._


/**
  */
class NestedDDFSuite extends ATestSuite {

  /*
  test("test JSON with primitiveFieldAndType") {

    val df:DataFrame = jsonRDD(primitiveFieldAndType)

    df.printSchema()
    println("Number of records: " + df.count())

  }
  */
  test("Loading a JSON dataset from a text file") {
    val path = "resources/test/sleep_data_sample.json"
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
    for(col <- selected_cols)
      println(col)

  }

}
