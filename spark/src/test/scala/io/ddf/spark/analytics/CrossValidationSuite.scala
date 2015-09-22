package io.ddf.spark.analytics

import org.junit.Assert._
import io.ddf.content.Schema
import io.ddf.spark.{SparkDDF, ATestSuite}
import scala.collection.JavaConversions._

/**
  */
class CrossValidationSuite extends ATestSuite {
  createTableAirline()

  test("random Split") {
    val arr = for {
      x <- 1 to 1000
    } yield (Array(x.asInstanceOf[Object]))

    val data = manager.getSparkContext.parallelize(arr, 2)
    val schema = new Schema("data", "v1 int");

    val ddf = new SparkDDF(manager, data, classOf[Array[Object]], manager.getNamespace, "data", schema)
    for (seed <- 1 to 5) {
      for (split <- ddf.ML.CVRandom(5, 0.85, seed)) {
        val train = split.getTrainSet.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]]).collect()
        val test = split.getTestSet.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]]).collect().toSet
        assertEquals(0.85, train.size / 1000.0, 0.025)
        assert(train.forall(x => !test.contains(x)), "train element found in test set!")
      }
    }
  }

  test("KFold Split") {
    val arr = for {
      x <- 1 to 5000
    } yield (Array(x.asInstanceOf[Object]))

    val data = manager.getSparkContext.parallelize(arr, 2)
    val schema = new Schema("data", "v1 int");

    val ddf = new SparkDDF(manager, data, classOf[Array[Object]],
      manager.getNamespace, "data1", schema)
    for (seed <- 1 to 3) {
      val betweenFolds = scala.collection.mutable.ArrayBuffer.empty[Set[Array[Object]]]
      for (split <- ddf.ML.CVKFold(5, seed)) {
        val train = split.getTrainSet.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]]).collect()
        val test = split.getTestSet.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]]).collect().toSet
        assertEquals(0.8, train.size / 5000.0, 0.02)
        assert(train.forall(x => !test.contains(x)), "train element found in test set!")
        betweenFolds += test
      }
      for (pair <- betweenFolds.toArray.combinations(2)) {
        val Array(a, b) = pair
        assert(a.intersect(b).isEmpty, "test set accross folds are not disjoint!")
      }
    }
  }

  test("test with airline table") {
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]
    val tableName = ddf.getTableName
    for (split <- ddf.ML.CVKFold(5, 10)) {

      val trainddf = split.getTrainSet.asInstanceOf[SparkDDF]
      val testddf = split.getTestSet.asInstanceOf[SparkDDF]

      val train = trainddf.getRDD(classOf[Array[Object]]).collect()
      val test = testddf.getRDD(classOf[Array[Object]]).collect().toSet
      assertEquals(0.8, train.size / 301.0, 0.1)
      assertEquals(0.2, test.size / 301.0, 0.1)

      //Ensure train's tableName and test's tableName are different
      val trainTableName = trainddf.getTableName
      val testTableName = testddf.getTableName

      assert(trainTableName != testTableName, "trainddf's tableName and testddf's tableName must be different")
      assert(trainTableName != tableName)
      assert(testTableName != tableName)
    }
  }
}
