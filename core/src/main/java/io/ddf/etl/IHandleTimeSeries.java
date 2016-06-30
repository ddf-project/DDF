package io.ddf.etl;

import java.util.List;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;
import java.util.concurrent.TimeUnit;

public interface IHandleTimeSeries extends IHandleDDFFunctionalGroup {

  void setTimeStampColumn(String colName);
  
  void setTsIDColumn(String colName);
  
  String getTimeStampColumn() throws DDFException;
  
  DDF downsample(String timestampColumn, List<String> aggregateFunctions, int interval, TimeUnit timeUnit) throws DDFException;
  
  DDF downsample(String timestampColumn, String tsIDColumn, List<String> aggregateFunctions, int interval, TimeUnit timeUnit) throws DDFException;
  
  DDF addDiffColumn(String timestampColumn, String colToGetDiff, String diffColName);
  
  DDF addDiffColumn(String timestampColumn, String tsIDColumn, String colToGetDiff, String diffColName) throws DDFException;
  
  DDF computeMovingAverage(String timestampColumn, String tsIDColumn, String colToComputeMovingAverage, String movingAverageColName, 
      int windowSize) throws DDFException;
  
  void persist_ts(String path);
  
}
