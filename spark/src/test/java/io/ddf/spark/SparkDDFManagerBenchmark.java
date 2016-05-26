package io.ddf.spark;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import io.ddf.hdfs.HDFSDDF;
import io.ddf.hdfs.HDFSDDFManager;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


public class SparkDDFManagerBenchmark {
  private static SparkDDFManager sparkDDFManager;
  private static HDFSDDFManager hdfsDDFManager;
  private static int numIteration = 1;

  static {
    try {
      sparkDDFManager = (SparkDDFManager) DDFManager.get(DDFManager.EngineType.SPARK);
      String hdfsURI = System.getenv("HDFS_URI");
      if (hdfsURI == null) hdfsURI = "hdfs:;";
      hdfsDDFManager = new HDFSDDFManager(hdfsURI);
    } catch (DDFException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void benchmarkLoadingORCTest() throws DDFException {
    Object url = "/test_pe/orc/kohls/";
    benchmarkTest(o -> {
      return loadingOrcFromHDFSToSpark(o);
    }, url, numIteration);

  }

  public void benchmarkTest(Function<Object, Long> fnBenchmark, Object params, int numIteration) {
    System.out.println("Start Benchmark");
    Long totalTime = 0L;
    for (int i = 0; i < numIteration; ++i) {
      totalTime += fnBenchmark.apply(params);
    }
    System.out.println("Loop:" + numIteration + " In " + totalTime + "(ms) - " + TimeUnit.MILLISECONDS.toMinutes(totalTime) + "(m)");
    System.out.println("Avg:" + totalTime / numIteration + "(ms) - " + TimeUnit.MILLISECONDS.toMinutes(totalTime/numIteration));
  }

  protected Long loadingOrcFromHDFSToSpark(Object hdfsURL) {
    long t1 = System.currentTimeMillis();
    HDFSDDF orcDDF = null;
    try {
      orcDDF = hdfsDDFManager.newDDF(String.valueOf(hdfsURL), null, null);
      DDF orcSparkDDF = sparkDDFManager.copyFrom(orcDDF);
      List<String> head = orcSparkDDF.VIEWS.head(100);
//      long numRows = orcSparkDDF.getNumRows();
      long totalTimeInMs = System.currentTimeMillis() - t1;
      System.out.println("Loading: " + hdfsURL);
//      System.out.println("Num Rows:" + numRows + " Total Time(ms):" + totalTimeInMs);
      return totalTimeInMs;
    } catch (DDFException e) {
      e.printStackTrace();
    }
    return 0L;
  }


}
