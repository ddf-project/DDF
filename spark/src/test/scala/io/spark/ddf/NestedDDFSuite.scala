package io.spark.ddf


import org.apache.spark.sql.{SQLContext, DataFrame}

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
    val sqlCtx = new SQLContext(manager.getSparkContext)
    val df:DataFrame = sqlCtx.jsonFile(path)

    println("dataframe created from json file at " + path)
    df.printSchema()
    println("Number of records: " + df.count())

  }

}
