package io.ddf2.spark.handlers.impl;

import io.ddf2.DDFException;
import io.ddf2.IDDF;

import java.util.List;

/**
 * Created by jing on 2/9/16.
 */
public class BinningHandler extends io.ddf2.handlers.impl.BinningHandler {
  public BinningHandler(IDDF associatedDDF) {
    super(associatedDDF);
  }

  @Override
  public IDDF binningCustom(String column, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
    return null;
  }

  @Override
  public IDDF binningEq(String column, int numBins, boolean includeLowest, boolean right) throws DDFException {
    return null;
  }

  @Override
  public IDDF binningEqFreq(String column, int numBins, boolean includeLowest, boolean right) throws DDFException {
    return null;
  }

  @Override
  public List<HistogramBin> getHistogram(String column, int numBins) throws DDFException {
    return null;
  }
}
