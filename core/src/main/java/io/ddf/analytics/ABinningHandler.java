package io.ddf.analytics;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;

import java.util.List;

public abstract class ABinningHandler extends ADDFFunctionalGroupHandler implements IHandleBinning {

  protected double[] breaks;

  public ABinningHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  public List<AStatisticsSupporter.HistogramBin> getVectorHistogram(String column, int numBins) throws DDFException{
    List<AStatisticsSupporter.HistogramBin> bins = getVectorHistogramImpl(column, numBins);
    return bins;
  }

  public abstract List<AStatisticsSupporter.HistogramBin> getVectorHistogramImpl(String column, int numBins)
          throws DDFException;

  public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
      boolean right) throws DDFException {

    DDF newddf = binningImpl(column, binningType, numBins, breaks, includeLowest, right);

    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;
  }

  public abstract DDF binningImpl(String column, String binningType, int numBins, double[] breaks,
      boolean includeLowest,
      boolean right) throws DDFException;

  public enum BinningType {
    CUSTOM, EQUAlFREQ, EQUALINTERVAL;

    public static BinningType get(String s) {
      if (s == null || s.length() == 0) return null;

      for (BinningType type : values()) {
        if (type.name().equalsIgnoreCase(s)) return type;
      }

      return null;
    }
  }
}
