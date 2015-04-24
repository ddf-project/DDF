package io.spark.ddf.analytics

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


  override def getVectorHistogramImpl(columnName: String, numBins: Int): java.util.List[AStatisticsSupporter.HistogramBin] = {
    val projectedDDF: DDF = mDDF.VIEWS.project(columnName)
    val rdd: RDD[Row] = projectedDDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val doubleRDD: DoubleRDDFunctions = new DoubleRDDFunctions(rdd.map(x => x.get(0).toString.toDouble))

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

  override def binningImpl(column: String, binningTypeString: String, numBins: Int, inputBreaks: Array[Double], includeLowest: Boolean,
                           right: Boolean): DDF = {

    val colMeta = mDDF.getColumn(column)

    val binningType = BinningType.get(binningTypeString)

    breaks = inputBreaks;

    binningType match {
      case BinningType.CUSTOM ⇒ {
        if (breaks == null) throw new DDFException("Please enter valid break points")
        if (breaks.sorted.deep != breaks.deep) throw new DDFException("Please enter increasing breaks")
      }
      case BinningType.EQUAlFREQ ⇒ breaks = {
        if (numBins < 2) throw new DDFException("Number of bins cannot be smaller than 2")
        getQuantilesFromNumBins(colMeta.getName, numBins)
      }
      case BinningType.EQUALINTERVAL ⇒ breaks = {
        if (numBins < 2) throw new DDFException("Number of bins cannot be smaller than 2")
        getIntervalsFromNumBins(colMeta.getName, numBins)
      }
      case _ ⇒ throw new DDFException(String.format("Binning type %s is not supported", binningTypeString))
    }
    //    mLog.info("breaks = " + breaks.mkString(", "))

    var intervals = createIntervals(breaks, includeLowest, right)

    var newddf = mDDF.getManager().sql2ddf(createTransformSqlCmd(column, intervals, includeLowest, right))

    //    mDDF.getManager().addDDF(newddf)

    //remove single quote in intervals
    intervals = intervals.map(x ⇒ x.replace("'", ""))
    newddf.getSchemaHandler().setAsFactor(column).setLevels(intervals.toList.asJava);

    newddf
  }

  def createIntervals(breaks: Array[Double], includeLowest: Boolean, right: Boolean): Array[String] = {
    val decimalPlaces: Int = 2
    val formatter = new DecimalFormat("#." + Iterator.fill(decimalPlaces)("#").mkString(""))
    var intervals: Array[String] = null
    intervals = (0 to breaks.length - 2).map {
      i ⇒
        if (right)
          "'(%s,%s]'".format(formatter.format(breaks(i)), formatter.format(breaks(i + 1)))
        else
          "'[%s,%s)'".format(formatter.format(breaks(i)), formatter.format(breaks(i + 1)))
    }.toArray
    if (includeLowest) {
      if (right)
        intervals(0) = "'[%s,%s]'".format(formatter.format(breaks(0)), formatter.format(breaks(1)))
      else
        intervals(intervals.length - 1) = "'[%s,%s]'".format(formatter.format(breaks(breaks.length - 2)), formatter.format(breaks(breaks.length - 1)))
    }
    mLog.info("interval labels = {}", intervals)
    intervals
  }

  def createTransformSqlCmd(col: String, intervals: Array[String], includeLowest: Boolean, right: Boolean): String = {
    val sqlCmd = "SELECT " + mDDF.getSchemaHandler().getColumns.map {
      ddfcol ⇒
        if (!col.equals(ddfcol.getName)) {
          ddfcol.getName
        }
        else {
          val b = breaks.map(_.asInstanceOf[Object])

          val caseLowest = if (right) {
            if (includeLowest)
              String.format("when ((%s >= %s) and (%s <= %s)) then %s ", col, b(0), col, b(1), intervals(0))
            else
              String.format("when ((%s > %s) and (%s <= %s)) then %s ", col, b(0), col, b(1), intervals(0))
          }
          else {
            String.format("when ((%s >= %s) and (%s < %s)) then %s ", col, b(0), col, b(1), intervals(0))
          }

          // all the immediate breaks
          val cases = (1 to breaks.length - 3).map {
            i ⇒
              if (right)
                String.format("when ((%s > %s) and (%s <= %s)) then %s ", col, b(i), col, b(i + 1), intervals(i))
              else
                String.format("when ((%s >= %s) and (%s < %s)) then %s ", col, b(i), col, b(i + 1), intervals(i))
          }.mkString(" ")

          val caseHighest = if (right) {
            String.format("when ((%s > %s) and (%s <= %s)) then %s ", col, b(b.length - 2), col, b(b.length - 1), intervals(intervals.length - 1))
          }
          else {
            if (includeLowest)
              String.format("when ((%s >= %s) and (%s <= %s)) then %s ", col, b(b.length - 2), col, b(b.length - 1), intervals(intervals.length - 1))
            else
              String.format("when ((%s >= %s) and (%s < %s)) then %s ", col, b(b.length - 2), col, b(b.length - 1), intervals(intervals.length - 1))
          }

          // the full case expression under select
          "case " + caseLowest + cases + caseHighest + " else null end as " + col
        }
    }.mkString(", ") + " FROM " + mDDF.getTableName
    mLog.info("Transform sql = {}", sqlCmd)

    sqlCmd
  }

  def getIntervalsFromNumBins(colName: String, bins: Int): Array[Double] = {
    val cmd = "SELECT min(%s), max(%s) FROM %s".format(colName, colName, mDDF.getTableName)
    val res: Array[Double] = mDDF.sql2txt(cmd, "").get(0).replace("ArrayBuffer", "").
      replace("(", "").replace(")", "").replace(", ", "\t").split("\t").map(x ⇒ x.toDouble)
    val (min, max) = (res(0), res(1))
    val eachInterval = (max - min) / bins
    val probs: Array[Double] = Array.fill[Double](bins + 1)(0)
    var i = 0
    while (i < bins + 1) {
      probs(i) = min + i * eachInterval
      i += 1
    }
    probs(bins) = max
    probs
  }

  def getQuantilesFromNumBins(colName: String, bins: Int): Array[Double] = {
    val eachInterval = 1.0 / bins
    val probs: Array[Double] = Array.fill[Double](bins - 1)(0.0)
    var i = 0
    while (i < bins - 1) {
      probs(i) = (i + 1) * eachInterval
      i += 1
    }
    getQuantiles(colName, probs)
  }

  /**
   * Using hive UDF to get percentiles as breaks
   *
   */
  def getQuantiles(colName: String, pArray: Array[Double]): Array[Double] = {
    var cmd = ""
    pArray.foreach(x ⇒ cmd = cmd + x.toString + ",")
    cmd = cmd.take(cmd.length - 1)
    cmd = String.format("min(%s), percentile_approx(%s, array(%s)), max(%s)", colName, colName, cmd, colName)
    mDDF.sql2txt("SELECT " + cmd + " FROM @this", "").get(0).replaceAll("ArrayBuffer|\\(|\\)|,", "").split("\t| ").
      map(x ⇒ x.toDouble)
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
