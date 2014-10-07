package io.spark.ddf.ml

import org.apache.spark.rdd.RDD
import io.ddf.ml.RocMetric
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkContext._

class ROCComputer extends Serializable {

  def ROC(XYData: RDD[LabeledPoint], alpha_length: Int): RocMetric = {
    var alpha: Array[Double] = new Array[Double](alpha_length)
    //    XYData.mapPartitions(f, preservesPartitioning)
    var roc = XYData.mapPartitions(mappingPredictToThreshold(alpha_length)).reduce(_.addIn(_))

    // TODO: check of roc is null
    var pred = roc.pred
    var previousVal: Double = Double.MaxValue
    var P: Double = 0.0
    var N: Double = 0.0

    //count number of positve, negative test instance
    var c: Int = 0
    // c= number of partition

    //check if pred.length == 0 or not
    if (pred.length > 0) {
      while (c < pred.length) {
        if (pred(c) != null) {
          P = P + pred(c)(1)
          N = N + pred(c)(2)
        }
        c = c + 1
      }
    }
    else {
      throw new IllegalArgumentException("Please try to run on binary classification model or contact system operators for assistance");
    }

    var result: Array[Array[Double]] = new Array[Array[Double]](pred.size)
    var i: Int = 0
    var count: Int = 0

    var tp: Double = 0.0
    var fp: Double = 0.0
    var accumulatetp: Double = 0.0
    var accumulatefp: Double = 0.0
    var accumulatefn: Double = 0.0
    var accumulatetn: Double = 0.0

    i = pred.length - 1
    var lastNotNullIndex: Int = 0
    //final result, 'merge' all intermediate result
    while (i >= 0) {
      //0 index is for threshold value
      if (pred(i) != null) {

        if (lastNotNullIndex == 0) lastNotNullIndex = i

        tp = pred(i)(1)
        fp = pred(i)(2)

        accumulatetp = accumulatetp + tp
        accumulatefp = accumulatefp + fp

        result(i) = new Array[Double](9)
        result(i)(0) = pred(i)(0)

        //true positive rate
        if (P != 0) {
          result(i)(1) = (accumulatetp / P).asInstanceOf[Double]
        } //P == 0 meaning, all accumulatetp = 0, therefore 0/0, let make it 0
        else {
          result(i)(1) = accumulatetp
        }
        //false positive rate
        if (N != 0) {
          result(i)(2) = (accumulatefp / N).asInstanceOf[Double]
        }
        else {
          result(i)(2) = accumulatefp
        }

        //precision
        result(i)(3) = (accumulatetp / (accumulatetp + accumulatefp)).asInstanceOf[Double]
        //recall is same as tpr
        result(i)(4) = result(i)(1)

        accumulatefn = P - accumulatetp
        accumulatetn = N - accumulatefp

        //sensitivity is the same as true positive rate
        result(i)(5) = result(i)(1)
        //specificity is the same as true negative rate
        result(i)(6) = (accumulatetn / (accumulatetn + accumulatefp)).asInstanceOf[Double]

        //f1 score
        result(i)(7) = (2 * accumulatetp / (2 * accumulatetp + accumulatefp + accumulatefn)).asInstanceOf[Double]
        //accuracy
        result(i)(8) = (accumulatetp + accumulatetn) / (accumulatetp + accumulatetn + accumulatefp + accumulatefn)
        count = count + 1
      }
      i = i - 1
    }

    //filter null/NA in pred
    //    var result2: Array[Array[Double]] = new Array[Array[Double]](count)
    val result2: Array[Array[Double]] = new Array[Array[Double]](pred.length)
    i = 0
    var j: Int = 0
    var previousTpr: Double = 0
    var previousFpr: Double = 0
    //    var auc: Double = 0

    while (i < pred.length) {
      //0 index is for threshold value
      if (pred(i) != null) {
        result2(j) = result(i)
        var threshold: Double = i * 1 / alpha_length.asInstanceOf[Double]
        result2(j)(0) = threshold //pred(i)(0)
        //accumulate auc
        //update previous
        previousTpr = result2(j)(1)
        previousFpr = result2(j)(2)
        j = j + 1
      } //in case some threshold data points are null, we just automatically repeated the lastNonNull metrics
      //the purpose is to make the data much more comprehensive for users
      else if (i >= lastNotNullIndex + 1) {
        result2(j) = new Array[Double](result(lastNotNullIndex).length)
        var t: Int = 1
        while (t < result(lastNotNullIndex).length) {
          result2(j)(t) = result(lastNotNullIndex)(t)
          t += 1
        }
        var threshold: Double = i * 1 / alpha_length.asInstanceOf[Double]
        result2(j)(0) = threshold //pred(i)(0)
        j = j + 1
      }
      i = i + 1
    }
    var ret: RocMetric = new RocMetric(result2, 0.0)
    ret.computeAUC

    ret
  }

  /*
   * compute TP, FP for each prediction partition
   * input: partition <Vector, Vector>
   * output: Array: alpha threshold, tp, fp
   * 
   * !!! NOTE: the algorithm implicitly assume the yTrue is 1st column and predict is 2nd column
   * 
   * Array: length = alpha_length
   * each element: threshold, positve_frequency, negative_frequency
   */
  def mappingPredictToThreshold(alpha_length: Int)(input: Iterator[LabeledPoint]): Iterator[RocMetric] = {
    //loop thorugh all test instance
    var output: Array[Array[Double]] = new Array[Array[Double]](alpha_length)
    var predict = 0.0
    var yTrue = 0.0
    var index: Int = 0
    var threshold: Double = 0.0

    while (input.hasNext) {
      val point = input.next()
      yTrue = point.features(0)
      predict = point.label

      //model.predict(Vector(input._1.getRow(i)))
      index = getAlpha(predict, alpha_length)

      threshold = getThreshold(predict, alpha_length)

      //update
      if (output(index) != null) {
        output(index)(0) = threshold
        if (yTrue == 1.0) {
          //positve_frequency++
          output(index)(1) = output(index)(1) + 1.0
        }
        else {
          //negative_frequency++
          output(index)(2) = output(index)(2) + 1.0
        }
      } //create new element
      else {
        output(index) = new Array[Double](3)
        output(index)(0) = threshold
        if (yTrue == 1.0) {
          //positve_frequency++
          output(index)(1) = 1.0
        }
        else {
          //negative_frequency++
          output(index)(2) = 1.0
        }
      }
    }
    var roc = new RocMetric(output, 0.0)
    Iterator(roc)
  }

  /*
   * get threshold in alpha array
   */
  def getAlpha(score: Double, alpha_length: Int): Int = {
    var index: Int = math.floor((score * alpha_length).asInstanceOf[Double]).asInstanceOf[Int]
    //re-indexing if score is precisely 1.0 to avoid ArrayOutofIndex exception
    if (index == alpha_length)
      index = index - 1
    index
  }

  /*
   * 
   */
  def getThreshold(predict: Double, alpha_length: Int): Double = {
    var index = getAlpha(predict, alpha_length)
    index * 1 / alpha_length.asInstanceOf[Double]
  }

  //  def AucScore(model: LogisticRegressionModel, XYData: RDD[(Matrix, Vector)]) {
  //    1.23345 // TODO(khang)
  //  }

  def computeRmse(data: RDD[Rating], predictions: RDD[Rating], implicitPrefs: Boolean): Double = {
    val predictionsAndRatings = predictions.map { x ⇒
      ((x.user, x.product), mapPredictedRating(x.rating, implicitPrefs))
    }.join(data.map(x ⇒ ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x ⇒ (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  def mapPredictedRating(r: Double, implicitPrefs: Boolean) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
}
