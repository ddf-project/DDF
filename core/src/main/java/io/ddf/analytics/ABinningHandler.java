package io.ddf.analytics;

import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.MathArrays;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public abstract class ABinningHandler extends ADDFFunctionalGroupHandler implements IHandleBinning {

  protected double[] breaks;

  public ABinningHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  public abstract List<AStatisticsSupporter.HistogramBin> getVectorHistogram(String column, int numBins)
          throws DDFException;

  public abstract List<AStatisticsSupporter.HistogramBin> getVectorApproxHistogram(String column, int numBins)
          throws DDFException;

  public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean toLabels, boolean includeLowest,
      boolean right, int precision) throws DDFException {

    Schema.Column colMeta = this.getDDF().getColumn(column);

    BinningType binType = BinningType.get(binningType);

    double[] theBreaks = breaks;

    switch(binType) {
      case CUSTOM:
        if (breaks == null) throw new DDFException("Please enter valid break points");
        // check for monotonic
        Arrays.sort(theBreaks);
        if (!Arrays.equals(theBreaks, breaks)) throw new DDFException("Please enter increasing breaks");
        break;
      case EQUALFREQ:
        if (numBins < 2) throw new DDFException("Number of bins cannot be smaller than 2");
        theBreaks = getQuantilesFromNumBins(colMeta.getName(), numBins);
        includeLowest = true;
        right = false;
        break;
      case EQUALINTERVAL:
        if (numBins < 2) throw new DDFException("Number of bins cannot be smaller than 2");
        theBreaks = getIntervalsFromNumBins(colMeta.getName(), numBins, right);
        break;
      default:throw new DDFException(String.format("Binning type %s is not supported", binningType));
    }

    // Check for uniqueness
    List<Double> listBreaks = Arrays.asList(ArrayUtils.toObject(theBreaks));
    if (new HashSet<>(listBreaks).size() < theBreaks.length) {
      throw new DDFException("Breaks must be unique: [" + StringUtils.join(listBreaks, ", ") + "]");
    }

    // Check if precision is a non-negative one
    if (precision < 0) throw new DDFException("precision must be a positive value");

    String[] intervals = createIntervals(theBreaks, includeLowest, right, precision);

    DDF newDDF = this.getDDF().getManager().sql2ddf(createTransformSqlCmd(column, theBreaks, intervals, toLabels, includeLowest, right), this.getDDF().getEngine());

    // If the output are interval values, make the resulted column a factor
    if (toLabels) {
      //remove single quote in intervals
      for(int i = 0; i < intervals.length; i++) {
        intervals[i] = intervals[i].replace("'", "");
      }
      List<Object> levels = Arrays.asList(intervals);
      newDDF.getSchemaHandler().setAsFactor(column).setLevels(levels);
    }

    newDDF.getMetaDataHandler().copyFactor(this.getDDF());
    return newDDF;
  }

  private double[] getIntervalsFromNumBins(String colName, int numBins, Boolean right) throws DDFException {
    DDF ddf = this.getDDF();
    double[] minMax = new double[] {ddf.getVectorMin(colName), ddf.getVectorMax(colName)};
    double min = minMax[0];
    double max = minMax[1];
    if (min == max) {
      min -= 0.001 * min;
      max += 0.001 * max;
      return getBins(min, max, numBins);
    } else {
      double[] bins = getBins(min, max, numBins);
      double adj = (max - min) * 0.001;
      if (right) {
        bins[0] -= adj;
      } else {
        bins[numBins - 1] += adj;
      }
      return bins;
    }
  }

  private double[] getBins(double min, double max, int numBins) {
    double eachInterval = (max - min) / numBins;
    double[] probs = new double[numBins + 1];
    int i = 0;
    while (i < numBins) {
      probs[i] = min + i * eachInterval;
      i += 1;
    }
    probs[numBins] = max;
    return probs;
  }

  private double[] getQuantilesFromNumBins(String colName, int numBins) throws DDFException {
    double eachInterval = 1.0 / numBins;
    double[] probs = new double[numBins + 1];
    int i = 0;
    while (i < numBins + 1) {
      probs[i] = i * eachInterval;
      i += 1;
    }
    probs[numBins] = 1;
    return ArrayUtils.toPrimitive(this.getDDF().getStatisticsSupporter().getVectorQuantiles(colName, ArrayUtils.toObject(probs)));
  }

  private String[] createIntervals(double[] breaks, Boolean includeLowest, Boolean right, int precision) throws DDFException {
    // Search for good precision with which every bin's breaks' formatted strings are distinguishable
    int tries = 1;
    DecimalFormat formatter;
    while (true) {
      if (tries > 20) throw new DDFException("Cannot find a good precision value for distinct break values");
      formatter = new DecimalFormat("#." + StringUtils.repeat("#", precision));
      int i;
      for(i = 0; i < breaks.length - 1; i++) {
        if ((breaks[i] != breaks[i+1]) && (formatter.format(breaks[i]).equals(formatter.format(breaks[i+1])))) {
          precision++;
          tries++;
          break;
        }
      }

      // All are distinguishable, quit
      if (i == breaks.length - 1) break;
    }


    String[] intervals = new String[breaks.length - 1];
    for(int i = 0; i < breaks.length - 1; i++) {
      if (right) {
        intervals[i] = String.format("'(%s,%s]'", formatter.format(breaks[i]), formatter.format(breaks[i + 1]));
      } else {
        intervals[i] = String.format("'[%s,%s)'", formatter.format(breaks[i]), formatter.format(breaks[i + 1]));
      }

    }

    if (includeLowest) {
      if (right) {
        intervals[0] = String.format("'[%s,%s]'", formatter.format(breaks[0]), formatter.format(breaks[1]));
      } else {
        intervals[intervals.length - 1] = String.format("'[%s,%s]'", formatter.format(breaks[breaks.length - 2]), formatter.format(breaks[breaks.length - 1]));
      }
    }

    return intervals;
  }

  protected String createTransformSqlCmd(String column, double[] breaks, String[] intervals, boolean toLabels, boolean includeLowest, boolean right) {
    List<Schema.Column> columns = this.getDDF().getSchemaHandler().getColumns();

    List<String> binningColumns = new ArrayList<String>(columns.size());

    for(int i = 0; i < columns.size(); i++) {
      String colName = columns.get(i).getName();
      if (!column.equals(colName)) {
        binningColumns.add(colName);
      } else {
        String caseLowest;
        if (right) {
          if (includeLowest) {
            caseLowest = String.format("when ((%s >= %s) and (%s <= %s)) then %s", column, breaks[0], column, breaks[1], toLabels?intervals[0]:"0");
          } else {
            caseLowest = String.format("when ((%s > %s) and (%s <= %s)) then %s", column, breaks[0], column, breaks[1], toLabels?intervals[0]:"0");
          }
        } else {
          caseLowest = String.format("when ((%s >= %s) and (%s < %s)) then %s", column, breaks[0], column, breaks[1], toLabels?intervals[0]:"0");
        }

        String caseMiddle;
        List<String> whenClauses = new ArrayList<>(breaks.length - 2);
        for(int j = 1; j < breaks.length - 2; j++) {
          if (right) {
            whenClauses.add(String.format("when ((%s > %s) and (%s <= %s)) then %s", column, breaks[j], column, breaks[j + 1], toLabels?intervals[j]:String.format("%d", j)));
          } else {
            whenClauses.add(String.format("when ((%s >= %s) and (%s < %s)) then %s", column, breaks[j], column, breaks[j + 1], toLabels?intervals[j]:String.format("%d", j)));
          }
        }
        caseMiddle = StringUtils.join(whenClauses, " ").toString();

        String caseHighest;
        if (right) {
          caseHighest = String.format("when ((%s > %s) and (%s <= %s)) then %s", column, breaks[breaks.length - 2], column, breaks[breaks.length - 1], toLabels?intervals[intervals.length - 1]:String.format("%d", intervals.length - 1));
        } else {
          if (includeLowest) {
            caseHighest = String.format("when ((%s >= %s) and (%s <= %s)) then %s", column, breaks[breaks.length - 2], column, breaks[breaks.length - 1], toLabels?intervals[intervals.length - 1]:String.format("%d", intervals.length - 1));
          } else {
            caseHighest = String.format("when ((%s >= %s) and (%s < %s)) then %s", column, breaks[breaks.length - 2], column, breaks[breaks.length - 1], toLabels?intervals[intervals.length - 1]:String.format("%d", intervals.length - 1));
          }
        }

        binningColumns.add(String.format("case %s %s %s else null end as %s", caseLowest, caseMiddle, caseHighest, column));
      }
    }

    return String.format("SELECT %s FROM %s", StringUtils.join(binningColumns, ", ").toString(), this.getDDF().getTableName());
  }

  public enum BinningType {
    CUSTOM, EQUALFREQ, EQUALINTERVAL;

    public static BinningType get(String s) {
      if (s == null || s.length() == 0) return null;

      for (BinningType type : values()) {
        if (type.name().equalsIgnoreCase(s)) return type;
      }

      return null;
    }
  }
}
