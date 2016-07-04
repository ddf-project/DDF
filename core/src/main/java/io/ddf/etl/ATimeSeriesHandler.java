package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.analytics.ABinningHandler.BinningType;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Lists;

public abstract class ATimeSeriesHandler extends ADDFFunctionalGroupHandler implements IHandleTimeSeries {

  protected String mTimestampColumn;
  protected String mTsIDColumn = null;


  public ATimeSeriesHandler(DDF theDDF) {
    super(theDDF);

  }

  public void setTimeStampColumn(String colName) {
    mTimestampColumn = colName;
  }

  public String getTimeStampColumn() {
    return mTimestampColumn;
  }


  public String getTsIDColumn() {
    return mTsIDColumn;
  }

  public void setTsIDColumn(String colName) {
    this.mTsIDColumn = colName;
  }

  @Override
  public DDF downsample(String timestampColumn, List<String> aggregateFunctions, int interval, TimeUnit timeUnit)
      throws DDFException {

    this.mTimestampColumn = timestampColumn;
    List<String> groupByCols = Lists.newArrayList(timestampColumn);
    if (mTsIDColumn != null && !mTsIDColumn.isEmpty()) {
      groupByCols.add(mTsIDColumn);
    }

    long intervalInSeconds = timeUnit.toSeconds(interval);

    int numBins = getNumBins(intervalInSeconds);
    DDF binnedDDF = this.getDDF().binning(timestampColumn, BinningType.EQUALINTERVAL.toString(), numBins, null, false,
        true, true);
    DDF newDDF = binnedDDF.groupBy(groupByCols, aggregateFunctions);

    return newDDF;
  }

  @Override
  public DDF downsample(String timestampColumn, String tsIDColumn, List<String> aggregateFunctions, int interval,
      TimeUnit timeUnit) throws DDFException {

    this.mTsIDColumn = tsIDColumn;
    List<String> rs = getDistinctValues(tsIDColumn);

    DDF ddf0 = filterByValue(tsIDColumn, rs.get(0));

    ddf0.getTimeSeriesHandler().setTsIDColumn(tsIDColumn);
    DDF newDDF = ddf0.getTimeSeriesHandler().downsample(timestampColumn, aggregateFunctions, interval, timeUnit);
    if (rs.size() > 1) {
      for (int i = 1; i < rs.size(); i++) {
        DDF filteredDDF = filterByValue(tsIDColumn, rs.get(i));
        filteredDDF.getTimeSeriesHandler().setTsIDColumn(tsIDColumn);
        DDF nextDDF = filteredDDF.getTimeSeriesHandler().downsample(timestampColumn, aggregateFunctions, interval,
            timeUnit);
        newDDF = newDDF.getJoinsHandler().merge(nextDDF);
      }
    }
    return newDDF;
  }

  @Override
  public DDF addDiffColumn(String timestampColumn, String colToGetDiff, String diffColumn) throws DDFException{
    return addDiffColumn(timestampColumn, null, colToGetDiff, diffColumn);
  }

  @Override
  public DDF addDiffColumn(String timestampColumn, String tsIDColumn, String colToGetDiff, String diffColumn)
      throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF computeMovingAverage(String timestampColumn, String tsIDColumn, String colToComputeMovingAverage,
      String movingAverageColName, int windowSize) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void save_ts(String pathToStorage) {
    // TODO Auto-generated method stub

  }

  private int getNumBins(long intervalInSeconds) throws DDFException {
    long minTimeStamp = this.getDDF().getVectorMin(mTimestampColumn).longValue();
    long maxTimeStamp = this.getDDF().getVectorMax(mTimestampColumn).longValue();
    int numBins = (int) ((maxTimeStamp - minTimeStamp) / intervalInSeconds);
    return numBins;

  }

  private List<String> getDistinctValues(String colName) throws DDFException {
    String sqlCmd = String.format("SELECT distinct(%s) FROM %s", colName, this.getDDF().getTableName());
    List<String> rs = this.getManager().sql(sqlCmd, this.getEngine()).getRows();
    return rs;
  }

  private DDF filterByValue(String colName, String value) throws DDFException {
    String sqlCmd = String.format("SELECT * FROM %s WHERE %s = '%s'", this.getDDF().getTableName(), colName, value);
    DDF filteredDDF = this.getDDF().getSqlHandler().sql2ddf(sqlCmd);
    return filteredDDF;
  }
}
