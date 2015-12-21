package io.ddf.spark.analytics

import io.ddf.DDF
import io.ddf.ml.IModel
import io.ddf.spark.{ATestSuite, SparkDDF}
import org.apache.spark.rdd._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  */
class MLSupporterSuite extends ATestSuite {
 /* createTableAirlineSmall()

  test("Test KMeans Prediction") {
    val ddf: DDF = manager.sql2ddf("select year, month, dayofmonth from airline", "SparkSQL")
    val k: Int = 5
    val numIterations: Int = 5
    val kmeansModel: IModel = ddf.ML.KMeans(5, 5, 2, "random")
    val pred: SparkDDF = ddf.ML.applyModel(kmeansModel, false, true).asInstanceOf[SparkDDF]

    val numrows = pred.getNumRows
    assert(numrows > 0)
    manager.shutdown
  }

  test("Test KMeans Prediction new Types") {
    createTableAirline_ColTypes()
    val ddf: DDF = manager.sql2ddf("select year, month, dayofmonth, flightnum from airline_type", "SparkSQL")
    val k: Int = 5
    val numIterations: Int = 5
    val kmeansModel: IModel = ddf.ML.KMeans(5, 5, 2, "random")
    val pred: SparkDDF = ddf.ML.applyModel(kmeansModel, false, true).asInstanceOf[SparkDDF]

    val numrows = pred.getNumRows
    assert(numrows > 0)
    manager.shutdown
  }
  */
  test("test mllib word2vec") {
    manager.sql("set shark.test.data.path=../resources", "SparkSQL")
    manager.sql("drop table if exists text8small", "SparkSQL")
    manager.sql("create table text8small (v1 string)" +
      " row format delimited fields terminated by ','", "SparkSQL")
    manager.sql("LOAD DATA LOCAL INPATH '${hiveconf:shark.test.data.path}/test/text8_small.txt' " +
      "INTO TABLE text8small", "SparkSQL")
  
    val ddf: DDF = manager.sql2ddf("select * from text8small","SparkSQL")
    val doc = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[String]])
    println(ddf.getNumRows)
    println(ddf.getNumColumns)
    println(doc.first)
    val input = doc.map{line => line.toSeq}
    val model = new Word2Vec().setMinCount(2).setVectorSize(10).setSeed(42L).fit(input)
    
    val kv = model.getVectors
    println(kv.head)
    println(kv.size)
    println(kv.keys)
    val syms = model.findSynonyms("feminism", 2) 
    assert(syms.length == 2)
    println(syms(0))
    println(syms(1))
    assert(syms(0)._2 > syms(1)._2)
    manager.shutdown
    
  }
}
