package io.ddf.analytics;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleBinning extends IHandleDDFFunctionalGroup {

  public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
      boolean right) throws DDFException;

}
