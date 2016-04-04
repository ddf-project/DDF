package io.ddf.spark.util;


import org.apache.spark.AccumulatorParam;

/**
 * Created by sangdn on 25/03/2016.
 */
public final class LongAccumulableParam implements AccumulatorParam<Long> {
 private static final LongAccumulableParam inst = new io.ddf.spark.util.LongAccumulableParam();
  public static LongAccumulableParam getInstance(){ return inst;}

  private LongAccumulableParam(){}
  @Override
  public Long addAccumulator(Long t1, Long t2) {
    return t1 + t2;
  }

  @Override
  public Long addInPlace(Long r1, Long r2) {
    return r1 + r2;
  }

  @Override
  public Long zero(Long initialValue) {
    return 0L;
  }
}