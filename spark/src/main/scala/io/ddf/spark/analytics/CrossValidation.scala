package io.spark.ddf.analytics

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD
import java.util.Random
import io.ddf.{DDFManager, DDF}
import java.util.{List => JList}
import io.ddf.exception.DDFException
import java.util
import io.ddf.content.Schema
import io.spark.ddf.SparkDDF
import org.apache.spark.sql.catalyst.expressions.Row

private[spark]
class SeededPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

private[spark]
class RandomSplitRDD[T: ClassManifest](
                                        prev: RDD[T],
                                        seed: Long,
                                        lower: Double,
                                        upper: Double,
                                        isTraining: Boolean)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new SeededPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SeededPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[SeededPartition]
    val rand = new Random(split.seed)
    // This is equivalent to taking a random sample without replacement
    // for for the train set and leave the rest as the test set.
    // The results are stable accross compute() calls due to deterministic RNG.
    if (isTraining) {
      firstParent[T].iterator(split.prev, context).filter(x => {
        val z = rand.nextDouble;
        z < lower || z >= upper
      })
    } else {
      firstParent[T].iterator(split.prev, context).filter(x => {
        val z = rand.nextDouble;
        lower <= z && z < upper
      })
    }
  }
}

object CrossValidation {
  /** Return an Iterator of size k of (train, test) RDD Tuple
    * for which the probability of each element belonging to each split is (trainingSize, 1-trainingSize).
    * The train & test data across k split are shuffled differently (different random seed for each iteration).
    */
  def randomSplit[T](rdd: RDD[T], numSplits: Int, trainingSize: Double, seed: Long)(implicit _cm: ClassManifest[T]): Iterator[(RDD[T], RDD[T])] = {
    require(0 < trainingSize && trainingSize < 1)
    val rg = new Random(seed)
    (1 to numSplits).map(_ => rg.nextInt).map(z =>
      (new RandomSplitRDD(rdd, z, 0, 1.0 - trainingSize, true),
        new RandomSplitRDD(rdd, z, 0, 1.0 - trainingSize, false))).toIterator
  }

  /** Return an Iterator of of size k of (train, test) RDD Tuple
    * for which the probability of each element belonging to either split is ((k-1)/k, 1/k).
    * The location of the test data is shifted consistently between folds
    * so that the resulting test sets are pair-wise disjoint.
    */
  def kFoldSplit[T](rdd: RDD[T], numSplits: Int, seed: Long)(implicit _cm: ClassManifest[T]): Iterator[(RDD[T], RDD[T])] = {
    require(numSplits > 0)
    (for (lower <- 0.0 until 1.0 by 1.0 / numSplits)
    yield (new RandomSplitRDD(rdd, seed, lower, lower + 1.0 / numSplits, true),
        new RandomSplitRDD(rdd, seed, lower, lower + 1.0 / numSplits, false))
      ).toIterator
  }

  def DDFRandomSplit(ddf: DDF, numSplits: Int, trainingSize: Double, seed: Long): JList[JList[DDF]] = {
    var unitType: Class[_] = null
    var splits: Iterator[(RDD[_], RDD[_])] = null
    if (ddf.getRepresentationHandler.has(classOf[RDD[_]], classOf[Row])) {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Row])
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Row]

    } else if (ddf.getRepresentationHandler.has(classOf[RDD[_]], classOf[Array[Double]])) {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]])
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Array[Double]]

    } else if (ddf.getRepresentationHandler.has(classOf[RDD[_]], classOf[Array[Object]])) {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]])
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Array[Object]]

    } else {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]])
      if (rdd == null) throw new DDFException("Cannot get RDD of Representation Array[Double], Array[Object] or Row")
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Array[Object]]
    }
    if (splits == null) {
      throw new DDFException("Error getting cross validation for DDF")
    }
    return getDDFCVSetsFromRDDs(splits, ddf.getManager, ddf.getSchema, ddf.getNamespace, unitType)
  }

  def DDFKFoldSplit(ddf: DDF, numSplits: Int, seed: Long): JList[JList[DDF]] = {
    var unitType: Class[_] = null
    var splits: Iterator[(RDD[_], RDD[_])] = null

    if (ddf.getRepresentationHandler.has(classOf[RDD[_]], classOf[Row])) {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Row])
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Row]

    } else if (ddf.getRepresentationHandler.has(Array(classOf[RDD[_]], classOf[Array[Double]]): _*)) {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]])
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Array[Double]]

    } else if (ddf.getRepresentationHandler.has(Array(classOf[RDD[_]], classOf[Array[Object]]): _*)) {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]])
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Array[Object]]

    } else {
      val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]])
      if (rdd == null) throw new DDFException("Cannot get RDD of representation Array[Double], Array[Object] or Row")
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Array[Object]]
    }

    if (splits == null) {
      throw new DDFException("Error getting cross validation for DDF")
    }
    return getDDFCVSetsFromRDDs(splits, ddf.getManager, ddf.getSchema, ddf.getNamespace, unitType)
  }

  /**
   * Get set of Cross Validation of DDFs from RDD splits
   * @param splits Iterator of tuple of (train, test) RDD
   * @param unitType unitType of returned DDF
   * @return List of Cross Validation sets
   */
  private def getDDFCVSetsFromRDDs(splits: Iterator[(RDD[_], RDD[_])], manager: DDFManager, schema: Schema, nameSpace: String, unitType: Class[_]): JList[JList[DDF]] = {
    val cvSets: JList[JList[DDF]] = new util.ArrayList[JList[DDF]]()

    for ((train, test) <- splits) {
      val aSet = new util.ArrayList[DDF]();

      val trainSchema = new Schema(null, schema.getColumns)

      val trainDDF = manager.newDDF(manager, train, Array(classOf[RDD[_]], unitType), nameSpace, null, trainSchema)

      val testSchema = new Schema(null, schema.getColumns)
      val testDDF = manager.newDDF(manager, test, Array(classOf[RDD[_]], unitType), nameSpace, null, testSchema)

      aSet.add(trainDDF)
      aSet.add(testDDF)
      cvSets.add(aSet)
    }
    return cvSets
  }
}
