package io.ddf.spark.analytics

import io.ddf.DDF
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import io.ddf.analytics.{AStatisticsSupporter, ABinningHandler, IHandleBinning}
import io.ddf.analytics.ABinningHandler._
import io.ddf.exception.DDFException
import java.text.DecimalFormat
import scala.annotation.tailrec
import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class BinningHandler(mDDF: DDF) extends ABinningHandler(mDDF) with IHandleBinning {


  def parseDouble(r: Row) = try {r.get(0).toString.toDouble } catch { case _ => None }

  override def getVectorHistogram(columnName: String, numBins: Int): java.util.List[AStatisticsSupporter.HistogramBin] = {
    val projectedDDF: DDF = mDDF.VIEWS.project(columnName)
    val rdd: RDD[Row] = projectedDDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val rdd1 = rdd.map(r => {try {r.get(0).toString.toDouble } catch { case _ => None }})
    val rdd2 = rdd1.filter(x => x!=None)
    val doubleRDD: DoubleRDDFunctions = new DoubleRDDFunctions(rdd2.asInstanceOf[RDD[Double]])
    val hist: (Array[Double], Array[Long]) = doubleRDD.histogram(numBins)
    val x: Array[Double] = hist._1
    val y: Array[Long] = hist._2
    val bins: ArrayBuffer[AStatisticsSupporter.HistogramBin] = new ArrayBuffer[AStatisticsSupporter.HistogramBin]()
    for (i <- 0 until y.length) {
      val bin: AStatisticsSupporter.HistogramBin = new AStatisticsSupporter.HistogramBin
      bin.setX(x(i).toDouble)
      bin.setY(y(i).toDouble)
      bins += bin
    }
    bins.toList.asJava

  }

  override def getVectorApproxHistogram(columnName: String, numBins: Int): java.util.List[AStatisticsSupporter.HistogramBin] = {
    val command: String = s"select histogram_numeric($columnName,$numBins) from @this"

    mLog.info(">>>> getVectorHistogram command = " + command)
    val ddf: DDF = this.getDDF.sql2ddf(command)
    mLog.info(ddf.VIEWS.head(1).toString)
    val rddbins = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]].collect
    val histbins = rddbins(0).get(0).asInstanceOf[Seq[Row]]

    val bins = histbins.map(r => {
      val b = new AStatisticsSupporter.HistogramBin
      b.setX(r.get(0).asInstanceOf[Double])
      b.setY(r.get(1).asInstanceOf[Double])
      b
    })
    return bins.toList.asJava
  }

  val MAX_LEVEL_SIZE = Integer.parseInt(System.getProperty("factor.max.level.size", "1024"))

  /* Class to produce intervals from array of stopping
   * and method findInterval(Double) return an interval for a given Double
   */

  class Intervals(val stopping: List[Double], private val includeLowest: Boolean = false, right: Boolean = true,
                  formatter: DecimalFormat) extends Serializable {
    val intervals = createIntervals(Array[(Double ⇒ Boolean, String)](), stopping, true)

    @tailrec
    private def createIntervals(result: Array[(Double ⇒ Boolean, String)], stopping: List[Double], first: Boolean): Array[(Double ⇒ Boolean, String)] = stopping match {
      case Nil ⇒ result
      case x :: Nil ⇒ result
      case x :: y :: xs ⇒ {
        if (includeLowest && right)
          if (first)
            createIntervals(result :+((z: Double) ⇒ z >= x && z <= y, "[" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)
          else
            createIntervals(result :+((z: Double) ⇒ z > x && z <= y, "(" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)

        else if (includeLowest && !right)
          if (xs == Nil)
            createIntervals(result :+((z: Double) ⇒ z >= x && z <= y, "[" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)
          else
            createIntervals(result :+((z: Double) ⇒ z >= x && z < y, "[" + formatter.format(x) + "," + formatter.format(y) + ")"), y :: xs, false)

        else if (!includeLowest && right)
          createIntervals(result :+((z: Double) ⇒ z > x && z <= y, "(" + formatter.format(x) + "," + formatter.format(y) + "]"), y :: xs, false)

        else
          createIntervals(result :+((z: Double) ⇒ z >= x && z < y, "[" + formatter.format(x) + "," + formatter.format(y) + ")"), y :: xs, false)
      }
    }

    def findInterval(aNum: Double): Option[String] = {
      intervals.find {
        case (f, y) ⇒ f(aNum)
      } match {
        case Some((x, y)) ⇒ Option(y)
        case None ⇒ Option(null)
      }
    }


  }

}
