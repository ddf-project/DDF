package io.ddf.analytics;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema.ColumnType;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.*;

public abstract class AStatisticsSupporter extends ADDFFunctionalGroupHandler implements ISupportStatistics {

  public AStatisticsSupporter(DDF theDDF) {
    super(theDDF);
  }


  private Summary[] basicStats;

  private SimpleSummary[] simpleSummary;

  protected abstract Summary[] getSummaryImpl() throws DDFException;

  protected abstract SimpleSummary[] getSimpleSummaryImpl() throws DDFException;

  public Summary[] getSummary() throws DDFException {
    this.basicStats = getSummaryImpl();
    return basicStats;
  }

  public SimpleSummary[] getSimpleSummary() throws DDFException {
    this.simpleSummary = this.getSimpleSummaryImpl();
    return simpleSummary;
  }

  @Override
  public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException {
    FiveNumSummary[] fivenums = new FiveNumSummary[columnNames.size()];

    List<String> specs = Lists.newArrayList();
    Set<String> stringColumns = new HashSet<String>();
    for (String columnName : columnNames) {
      String query = fiveNumFunction(columnName);
      if (query != null && query.length() > 0) {
        specs.add(query);
      } else stringColumns.add(columnName);
    }

    String command = String.format("SELECT %s FROM %%s", StringUtils.join(specs.toArray(new String[0]), ','));

    if (!Strings.isNullOrEmpty(command)) {
    mLog.info(">>>> command = " + command);
      // a fivenumsummary of an Int/Long column is in the format "[min, max, 1st_quantile, median, 3rd_quantile]"
      // each value can be a NULL
      // a fivenumsummary of an Double/Float column is in the format "min \t max \t[1st_quantile, median, 3rd_quantile]"
      // or "min \t max \t null"s
      String [] rs = new String[5];
      if (this.getDDF().getEngineType().equals(DDFManager.EngineType
              .ENGINE_SPARK)) {
        rs = this.getDDF().sql(command, String.format("Unable to get fivenum summary of the given columns from table %%s")).getRows().get(0).replaceAll("\\[|\\]| ", "").replaceAll(",", "\t").split("\t| ");
      } else {
        String[] percentiles = {"0.0", "1.0", "0.25", "0.5", "0.75"};
        String sql = "select percentile_DISC(%s) within group (order by " +
                columnNames.get(0) + ") over() from (" + this.getDDF()
                .getTableName() + ") TMP_FIVENUM";
        for (int i = 0; i < percentiles.length; ++i) {
          String query = String.format(sql, percentiles[i]);

          rs[i] = this.getDDF().getManager().sql(query, this.getDDF()
                  .getEngineType().toString()).getRows().get(0);

        }
      }

      int k = 0;
      for (int i = 0; i < columnNames.size(); i++) {
        if (stringColumns.contains(columnNames.get(i))) {
          fivenums[i] = new FiveNumSummary(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        } else {
          fivenums[i] = new FiveNumSummary(parseDouble(rs[5 * k]), parseDouble(rs[5 * k + 1]),
              parseDouble(rs[5 * k + 2]),
              parseDouble(rs[5 * k + 3]), parseDouble(rs[5 * k + 4]));
          k++;
        }
      }

      return fivenums;
    }

    return null;
  }

  @Override
  public Double[] getVectorVariance(String columnName) throws DDFException {
    Double[] sd = new Double[2];

    String command = String.format("select var_samp(%s) from @this", columnName);
    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF()
          .sql(command, String.format("Unable to compute the variance of the given column from table %%s")).getRows();
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        Double a = Double.parseDouble(result.get(0));
        sd[0] = a;
        sd[1] = Math.sqrt(a);
        return sd;
      }
    }
    return null;
  }

  @Override
  public Double getVectorMean(String columnName) throws DDFException {
    Double mean = 0.0;
    String command = String.format("select avg(%s) from @this", columnName);

    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF()
          .sql(command, String.format("Unable to compute the mean of the given column from table %%s")).getRows();
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        mean = Double.parseDouble(result.get(0));
        return mean;
      }
    }
    return null;
  }

  @Override
  public Double getVectorMin(String columnName) throws DDFException {
    Double min = 0.0;
    String command = String.format("select min(%s) from @this", columnName);

    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF()
          .sql(command, String.format("Unable to compute the minimum value of the given column from table %%s")).getRows();
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        min = Double.parseDouble(result.get(0));
        return min;
      }
    }
    return null;
  }

  @Override
  public Double getVectorMax(String columnName) throws DDFException {
    Double max = 0.0;
    String command = String.format("select max(%s) from @this", columnName);

    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF()
          .sql(command, String.format("Unable to compute the maximum value of the given column from table %%s")).getRows();
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        max = Double.parseDouble(result.get(0));
        return max;
      }
    }
    return null;
  }

  @Override
  public double getVectorCor(String xColumnName, String yColumnName) throws DDFException {
    double corr = 0.0;
    String command = String.format("select corr(%s, %s) from @this", xColumnName, yColumnName);
    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF().sql(command,
              String.format("Unable to compute correlation of %s and %s from table %%s", xColumnName, yColumnName)).getRows();
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        corr = Double.parseDouble(result.get(0));
        return corr;
      }
    }
    return Double.NaN;
  }

  @Override
  public double getVectorCovariance(String xColumnName, String yColumnName) throws DDFException {
    double cov = 0.0;
    String command = String.format("select  covar_samp(%s, %s) from @this", xColumnName, yColumnName);
    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF().sql(command,
          String.format("Unable to compute covariance of %s and %s from table %%s", xColumnName, yColumnName)).getRows();
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        System.out.println(">>>>> parseDouble: " + result.get(0));
        cov = Double.parseDouble(result.get(0));
        return cov;
      }
    }
    return Double.NaN;
  }

  private double parseDouble(String s) {
    mLog.info(">>>> parseDouble: " + s);
    return ("NULL".equalsIgnoreCase(s.trim())) ? Double.NaN : Double.parseDouble(s);
  }

  private String fiveNumFunction(String columnName) {
    ColumnType colType = this.getDDF().getColumn(columnName).getType();

    if(ColumnType.isIntegral(colType))
        return String.format("PERCENTILE(%s, array(0, 1, 0.25, 0.5, 0.75))", columnName);
    else if(ColumnType.isFractional(colType))
        return String.format("MIN(%s), MAX(%s), PERCENTILE_APPROX(%s, array(0.25, 0.5, 0.75))", columnName, columnName,
            columnName);
    return "";
  }

  public static class HistogramBin {
    private double x; // Bin center
    private double y; // Bin weight

    public double getX() {
      return x;
    }

    public void setX(double x) {
      this.x = x;
    }

    public double getY() {
      return y;
    }

    public void setY(double y) {
      this.y = y;
    }
  }


  public static class FiveNumSummary implements Serializable {

    private static final long serialVersionUID = 1L;
    private double mMin = 0;
    private double mMax = 0;
    private double mFirst_quantile = 0;
    private double mMedian = 0;
    private double mThird_quantile = 0;


    public FiveNumSummary() {

    }

    public FiveNumSummary(double mMin, double mMax, double first_quantile, double median, double third_quantile) {
      this.mMin = mMin;
      this.mMax = mMax;
      this.mFirst_quantile = first_quantile;
      this.mMedian = median;
      this.mThird_quantile = third_quantile;
    }

    public double getMin() {
      return mMin;
    }

    public void setMin(double mMin) {
      this.mMin = mMin;
    }

    public double getMax() {
      return mMax;
    }

    public void setMax(double mMax) {
      this.mMax = mMax;
    }

    public double getFirstQuantile() {
      return mFirst_quantile;
    }

    public void setFirstQuantile(double mFirst_quantile) {
      this.mFirst_quantile = mFirst_quantile;
    }

    public double getMedian() {
      return mMedian;
    }

    public void setMedian(double mMedian) {
      this.mMedian = mMedian;
    }

    public double getThirdQuantile() {
      return mThird_quantile;
    }

    public void setThirdQuantile(double mThird_quantile) {
      this.mThird_quantile = mThird_quantile;
    }

  }

  public Double[] getVectorQuantiles(String columnName, Double[] percentiles) throws DDFException {
    return getVectorQuantiles(columnName, percentiles, 10000);
  }

  public Double[] getVectorQuantiles(String columnName, Double[] percentiles, Integer B) throws DDFException {
    if (percentiles == null || percentiles.length == 0) {
      throw new DDFException("Cannot compute quantiles for empty percenties");
    }

    if (Strings.isNullOrEmpty(columnName)) {
      throw new DDFException("Column name must not be empty");
    }
    Set<Double> pSet = new HashSet(Arrays.asList(percentiles));
    boolean hasZero = pSet.contains(0.0);
    boolean hasOne = pSet.contains(1.0);
    pSet.remove(0.0);
    pSet.remove(1.0);
    List<Double> pList = new ArrayList(pSet);

    String pParams = "";
    ColumnType columnType = this.getDDF().getColumn(columnName).getType();
    mLog.info("Column type: " + columnType.name());

    List<String> qmm = new ArrayList<String>();

    if (!pList.isEmpty()) {
      if (ColumnType.isIntegral(columnType)) {
        pParams = "percentile(" + columnName + ", array(" + StringUtils.join(pList, ",") + "))";
      } else if (ColumnType.isFractional(columnType)) {
        pParams = "percentile_approx(" + columnName + ", array(" + StringUtils.join(pList, ",") + "), " + B.toString()
            + ")";
      } else {
        throw new DDFException("Only support numeric vectors!!!");
      }
      qmm.add(pParams);
    }

    if (hasZero)
      qmm.add("min(" + columnName + ")");

    if (hasOne)
      qmm.add("max(" + columnName + ")");


    String cmd = "SELECT " + StringUtils.join(qmm, ", ") + " FROM @this";
    mLog.info(">>>>>>>>>>>>>> Command String = " + cmd);


    List<String> rs = getDDF().sql(cmd, "Cannot get vector quantiles from SQL queries").getRows();
    if (rs == null || rs.size() == 0) {
      throw new DDFException("Cannot get vector quantiles from SQL queries");
    }
    String[] convertedResults = rs.get(0)
        .replaceAll("\\[|\\]| ", "").replaceAll(",", "\t")
        .replace("null", "NULL, NULL, NULL").split("\t");
    mLog.info("Raw info " + StringUtils.join(rs, "\n"));

    HashMap<Double, Double> mapValues = new HashMap<Double, Double>();
    try {
      for (int i = 0; i < pList.size(); i++) {
        mapValues.put(pList.get(i), Double.parseDouble(convertedResults[i]));
      }

      if (hasZero && hasOne) {
        mapValues.put(1.0, Double.parseDouble(convertedResults[convertedResults.length - 1]));
        mapValues.put(0.0, Double.parseDouble(convertedResults[convertedResults.length - 2]));
      } else if (hasOne) {
        mapValues.put(1.0, Double.parseDouble(convertedResults[convertedResults.length - 1]));
      } else if (hasZero) {
        mapValues.put(0.0, Double.parseDouble(convertedResults[convertedResults.length - 1]));
      }

    } catch (NumberFormatException nfe) {
      throw new DDFException("Cannot parse the returned values from vector quantiles query", nfe);
    }

    Double[] result = new Double[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      result[i] = mapValues.get(percentiles[i]);
    }
    return result;
  }
}
