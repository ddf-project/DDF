package io.ddf.analytics;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import io.ddf.DDF;
import io.ddf.content.Schema.ColumnType;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public abstract class AStatisticsSupporter extends ADDFFunctionalGroupHandler implements ISupportStatistics {

  public AStatisticsSupporter(DDF theDDF) {
    super(theDDF);
  }


  private Summary[] basicStats;


  protected abstract Summary[] getSummaryImpl() throws DDFException;



  public Summary[] getSummary() throws DDFException {
    this.basicStats = getSummaryImpl();
    return basicStats;
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
      // a fivenumsummary of an Int/Long column is in the format "[min, max, 1st_quantile, median, 3rd_quantile]"
      // each value can be a NULL
      // a fivenumsummary of an Double/Float column is in the format "min \t max \t[1st_quantile, median, 3rd_quantile]"
      // or "min \t max \t null"s
      String[] rs = this.getDDF()
          .sql2txt(command, String.format("Unable to get fivenum summary of the given columns from table %%s")).get(0)
          .replace("[", "").replace("]", "").replaceAll("\t", ",").replace("null", "NULL, NULL, NULL").split(",");


      int k = 0;
      for (int i = 0; i < columnNames.size(); i++) {
        if (stringColumns.contains(columnNames.get(i))) {
          fivenums[i] = new FiveNumSummary(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        } else {
          fivenums[i] = new FiveNumSummary(parseDouble(rs[5 * k]), parseDouble(rs[5 * k + 1]),
              parseDouble(rs[5 * k + 2]), parseDouble(rs[5 * k + 3]), parseDouble(rs[5 * k + 4]));
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
      List<String> result = this.getDDF().sql2txt(command,
          String.format("Unable to compute the variance of the given column from table %%s"));
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
      List<String> result = this.getDDF().sql2txt(command,
          String.format("Unable to compute the mean of the given column from table %%s"));
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        mean = Double.parseDouble(result.get(0));
        return mean;
      }
    }
    return null;
  }

  @Override
  public double getVectorCor(String xColumnName, String yColumnName) throws DDFException {
    double corr = 0.0;
    String command = String.format("select corr(%s, %s) from @this", xColumnName, yColumnName);
    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF().sql2txt(command,
          String.format("Unable to compute correlation of %s and %s from table %%s", xColumnName, yColumnName));
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
      List<String> result = this.getDDF().sql2txt(command,
          String.format("Unable to compute covariance of %s and %s from table %%s", xColumnName, yColumnName));
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        cov = Double.parseDouble(result.get(0));
        return cov;
      }
    }
    return Double.NaN;
  }

  @Override
  public List<HistogramBin> getVectorHistogram(String columnName, int numBins) throws DDFException {
    String command = String.format("select histogram_numeric(%s,%s) from @this", columnName, numBins);
    if (!Strings.isNullOrEmpty(command)) {
      List<String> result = this.getDDF().sql2txt(command,
          String.format("Unable to compute histogram of %s from table %%s", columnName));
      if (result != null && !result.isEmpty() && result.get(0) != null) {
        List<HistogramBin> bins = Lists.newArrayList();
        Gson gson = new Gson();
        for (String rs : result) {
          Type typeOfT = new TypeToken<List<HistogramBin>>() {}.getType();
          List<HistogramBin> tmpbins = gson.fromJson((String) rs, typeOfT);
          for (HistogramBin bin : tmpbins)
            bins.add(bin);
        }
        return bins;
      }
    }
    return null;
  }

  private double parseDouble(String s) {
    return ("NULL".equalsIgnoreCase(s.trim())) ? Double.NaN : Double.parseDouble(s);
  }

  private String fiveNumFunction(String columnName) {
    ColumnType colType = this.getDDF().getColumn(columnName).getType();

    switch (colType) {
      case INT:
      case LONG:
        return String.format("PERCENTILE(%s, array(0, 1, 0.25, 0.5, 0.75))", columnName);

      case DOUBLE:
      case FLOAT:
        return String.format("MIN(%s), MAX(%s), PERCENTILE_APPROX(%s, array(0.25, 0.5, 0.75))", columnName, columnName,
            columnName);

      default:
        return "";
    }

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

    String colType = getDDF().getSchema().getColumn(columnName).getType().name().toLowerCase();

    mLog.info("Column type: " + colType);
    List<Double> pValues = Arrays.asList(percentiles);
    Pattern p1 = Pattern.compile("(big|small|tiny|int)");
    Pattern p2 = Pattern.compile("(float|double)");

    String min = "";
    boolean hasZero = false;
    if (Double.compare(pValues.get(0), 0.0) == 0) {
      min = "min(" + columnName + ")";
      pValues = pValues.subList(1, pValues.size());
      hasZero = true;
    }

    boolean hasOne = false;
    String max = "";
    if (Double.compare(pValues.get(pValues.size() - 1), 1.0) == 0) {
      max = "max(" + columnName + ")";
      pValues = pValues.subList(0, pValues.size() - 1);
      hasOne = true;
    }

    String pParams = "";

    if (!pValues.isEmpty()) {
      if (p1.matcher(colType).matches()) {
        pParams = "percentile(" + columnName + ", array(" + StringUtils.join(pValues, ",") + "))";
      } else if (p2.matcher(colType).matches()) {
        pParams = "percentile_approx(" + columnName + ", array(" + StringUtils.join(pValues, ",") + "), "
            + B.toString() + ")";
      } else {
        throw new DDFException("Only support numeric verctors!!!");
      }
    }

    mLog.info("pParams = {}", pParams);
    List<String> qmm = new ArrayList<String>();
    if (!pValues.isEmpty()) {
      qmm.add(pParams);
    }
    if (min.length() > 0) {
      // pParams = min + ", " + pParams;
      qmm.add(min);
    }

    if (max.length() > 0) {
      qmm.add(max);
    }

    String cmd = "SELECT " + StringUtils.join(qmm, ", ") + " FROM @this";
    mLog.info(">>>>>>>>>>>>>> Command String = " + cmd);


    List<String> rs = getDDF().sql2txt(cmd, "Cannot get vector quantiles from SQL queries");
    if (rs == null || rs.size() == 0) {
      throw new DDFException("Cannot get vector quantiles from SQL queries");
    }
    String[] convertedResults = rs.get(0).replace("[", "").replace("]", "").replaceAll("\t", ",")
        .replace("null", "NULL, NULL, NULL").split(",");
    mLog.info("Raw info " + StringUtils.join(rs, "\n"));

    Double[] result = new Double[percentiles.length];
    HashMap<Double, Double> mapValues = new HashMap<Double, Double>();
    try {
      for (int i = 0; i < pValues.size(); i++) {
        mapValues.put(pValues.get(i), Double.parseDouble(convertedResults[i]));
      }
      if (hasZero) {
        mapValues.put(0.0, Double.parseDouble(convertedResults[convertedResults.length - 2]));
      }
      if (hasOne) {
        mapValues.put(1.0, Double.parseDouble(convertedResults[convertedResults.length - 1]));
      }
    } catch (NumberFormatException nfe) {
      throw new DDFException("Cannot parse the returned values from vector quantiles query", nfe);
    }
    for (int i = 0; i < percentiles.length; i++) {
      result[i] = mapValues.get(percentiles[i]);
    }
    return result;
  }
}
