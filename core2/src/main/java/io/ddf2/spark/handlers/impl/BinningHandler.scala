package io.ddf.spark.handlers.impl

import io.ddf2.{DDFException, DDF, DDF}
import io.ddf2.handlers.{IStatisticHandler, IBinningHandler}
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.text.DecimalFormat
import scala.annotation.tailrec
import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/*
class BinningHandler(mDDF: DDF) extends io.ddf2.handlers.impl.BinningHandler(mDDF) with IBinningHandler {


  def parseDouble(r: Row) = try {
    r.get(0).toString.toDouble
  } catch {
    case _ => None
  }

  override def getHistogram(columnName: String, numBins: Int): java.util.List[IBinningHandler.HistogramBin] = {
    val projectedDDF: DDF = mDDF.getViewHandler.project(columnName)
    val rdd: RDD[Row] = projectedDDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val rdd1 = rdd.map(r => {
      try {
        r.get(0).toString.toDouble
      } catch {
        case _ => None
      }
    })
    val rdd2 = rdd1.filter(x => x != None)
    val doubleRDD: DoubleRDDFunctions = new DoubleRDDFunctions(rdd2.asInstanceOf[RDD[Double]])
    val hist: (Array[Double], Array[Long]) = doubleRDD.histogram(numBins)
    val x: Array[Double] = hist._1
    val y: Array[Long] = hist._2
    val bins: ArrayBuffer[IBinningHandler.HistogramBin] = new ArrayBuffer[IBinningHandler.HistogramBin]()
    for (i <- 0 until y.length) {
      val bin: IBinningHandler.HistogramBin = new IBinningHandler.HistogramBin(x(i).toDouble, y(i).toDouble)
      bins += bin
    }
    bins.toList.asJava

  }

  override def getApproxHistogram(columnName: String, numBins: Int): java.util.List[IBinningHandler
  .HistogramBin] = {
    val command: String = s"select histogram_numeric($columnName,$numBins) from @this"
    val ddf: DDF = this.getDDF.sql2ddf(command)
    val rddbins = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]].collect
    val histbins = rddbins(0).get(0).asInstanceOf[Seq[Row]]

    val bins = histbins.map(r => {
      val b = new IBinningHandler.HistogramBin(r.get(0).asInstanceOf[Double], r.get(1).asInstanceOf[Double])
      b
    })
    return bins.toList.asJava
  }

  @throws(classOf[DDFException])
  override def binningCustom(columnName: String, breaks: Array[Double], includeLowest: Boolean, right: Boolean)
  : DDF = {
    assert(breaks != null && breaks.size != 0)
    val column = mDDF.getSchema.getColumn(columnName)
    if (breaks.sorted.deep != breaks.deep) throw new DDFException("Please enter increasing breaks")

    var intervals = createIntervals(breaks, includeLowest, right)

    var newddf = mDDF.sql2ddf(createTransformSqlCmd(columnName, breaks, intervals, includeLowest, right))
    //remove single quote in intervals
    intervals = intervals.map(x ⇒ x.replace("'", ""))
    // TODO: factor
    // newddf.getSchemaHandler().setAsFactor(column).setLevels(intervals.toList.asJava);
    newddf
  }

  @throws(classOf[DDFException])
  override def binningEq(columnName: String, numBins: Int, includeLowest: Boolean, right: Boolean) : DDF = {
    assert(numBins >= 2)

  }

  def getBreaks(columnName: String, numBins: Int): Array[Double] = {
    val interval = 1.0 / numBins
    val probs : Array[Double] = Array.fill[Double](numBins - 1)(0.0)
    var i = 0
    while (i < numBins - 1) {
      probs(i) = (i + 1) * interval
      i = i + 1
    }

    var fields = probs.map(dbl => dbl.toString).mkString(", ")
    var value_extract = probs.view.zipWithIndex.map({case (x,i) => s"ps[$i]"}).mkString(", ")
    // TODO, not good to define table name in query, can have conflict
    val sqlcmd = String.format("SELECT minval, %s, maxval FROM (SELECT MIN(%s) AS minval,  " +
      "PERCENTILE_APPROX(%s, ARRAY(%s)) AS ps, MAX(%s) AS maxval FROM @this) quantiles",
      value_extract,
      columnName,
      columnName,
      fields,
      columnName
    )
    val rs = this.getDDF.sql(sqlcmd)
    if (rs.next()) {
      List(0, 1, 2).map(i => rs.getDouble(i))
    }
    throw new DDFException("")
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
    intervals
  }

  def createTransformSqlCmd(columnName: String, breaks: Array[Double], intervals: Array[String], includeLowest:
  Boolean, right: Boolean):
  String = {
    val sqlCmd = "SELECT " + mDDF.getSchema.getColumnNames.map {
      ddfcol =>
        if (!columnName.equalsIgnoreCase(ddfcol)) {
          ddfcol
        }
        else {
          val b = breaks.map(_.asInstanceOf[Object])

          val caseLowest = if (right) {
            if (includeLowest)
              String.format("WHEN ((%s >= %s) AND (%s <= %s)) THEN %s ", columnName, b(0), columnName, b(1), intervals(0))
            else
              String.format("WHEN ((%s > %s) AND (%s <= %s)) THEN %s ", columnName, b(0), columnName, b(1), intervals(0))
          }
          else {
            String.format("WHEN ((%s >= %s) AND (%s < %s)) then %s ", columnName, b(0), columnName, b(1), intervals(0))
          }

          // all the immediate breaks
          val cases = (1 to breaks.length - 3).map {
            i =>
              if (right)
                String.format("WHEN ((%s > %s) AND (%s <= %s)) THEN %s ", columnName, b(i), columnName, b(i + 1),
                  intervals(i))
              else
                String.format("WHEN ((%s >= %s) AND (%s < %s)) THEN %s ", columnName, b(i), columnName, b(i + 1),
                  intervals
                  (i))
          }.mkString(" ")

          val caseHighest = if (right) {
            String.format("WHEN ((%s > %s) AND (%s <= %s)) THEN %s ", columnName, b(b.length - 2), columnName, b(b.length - 1), intervals(intervals.length - 1))
          }
          else {
            if (includeLowest)
              String.format("WHEN ((%s >= %s) AND (%s <= %s)) THEN %s ", columnName, b(b.length - 2), columnName, b(b.length - 1), intervals(intervals.length - 1))
            else
              String.format("WHEN ((%s >= %s) AND (%s < %s)) THEN %s ", columnName, b(b.length - 2), columnName, b(b.length - 1), intervals(intervals.length - 1))
          }

          // the full case expression under select
          "CASE " + caseLowest + cases + caseHighest + " ELSE NULL END AS " + columnName
        }
    }.mkString(", ") + " FROM " + mDDF.getDDFName

    sqlCmd
  }

  def getIntervalsFromNumBins(colName: String, bins: Int): Array[Double] = {
    val cmd = "SELECT min(%s), max(%s) FROM %s".format(colName, colName, "@this")
    val res: Array[Double] = mDDF.sql(cmd, "").getRows.get(0).split("\t").map(x ⇒ x.toDouble)
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
    var value_extract = ""
    pArray.view.zipWithIndex.foreach({ case (x, i) => {
      cmd = cmd + x.toString + ","
      value_extract = value_extract + s"ps[$i],"
    }
    })
    cmd = cmd.take(cmd.length - 1)
    value_extract = value_extract.take(value_extract.length - 1)
    cmd = String.format("min(%s) as minval, percentile_approx(%s, array(%s)) as ps, max(%s) as maxval", colName, colName, cmd, colName)
    mDDF.sql("SELECT minval," + value_extract + ", maxval FROM (SELECT " + cmd + " FROM @this) quantiles", "").getRows.get(0).replaceAll("\\[|\\]| ", "").replaceAll(",", "\t").split("\t").
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

*/