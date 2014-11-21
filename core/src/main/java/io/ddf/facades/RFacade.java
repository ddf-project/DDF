package io.ddf.facades;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.analytics.IHandleAggregation;
import io.ddf.exception.DDFException;
import io.ddf.types.AggregateTypes.AggregateField;
import io.ddf.types.AggregateTypes.AggregateFunction;
import io.ddf.types.AggregateTypes.AggregationResult;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RFacade implements IHandleAggregation {

  private DDF mDDF;
  private IHandleAggregation mAggregationHandler;


  public RFacade(DDF ddf, IHandleAggregation aggregationHandler) {
    mDDF = ddf;
    mAggregationHandler = aggregationHandler;
  }

  public IHandleAggregation getAggregationHandler() {
    return mAggregationHandler;
  }

  public void setAggregationHandler(IHandleAggregation AggregationHandler) {
    mAggregationHandler = AggregationHandler;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }


  @Override
  public double computeCorrelation(String columnA, String columnB) throws DDFException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public AggregationResult aggregate(List<AggregateField> fields) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  // ///// Aggregate operations

  // aggregate(cbind(mpg,hp) ~ vs + am, mtcars, FUN=mean)
  public AggregationResult aggregate(String rAggregateFormula) throws DDFException {

    return mAggregationHandler.aggregate(AggregateField.fromSqlFieldSpecs(parseRAggregateFormula(rAggregateFormula)));
  }

  public static String parseRAggregateFormula(String rAggregateFormula) {

    List<String> aggregatedFields = Lists.newArrayList();

    String[] parts = rAggregateFormula.split("~");

    String[] rParts = parts[1].trim().split(",");

    if (parts[0].contains("cbind")) { // multiple aggregated fields
      Matcher matcher = Pattern.compile("^\\s*cbind\\((.+)\\)").matcher(parts[0].trim());

      if (matcher.matches()) {
        String[] aggregatedFieldArr = matcher.group(1).split(",");
        for (String field : aggregatedFieldArr) {
          aggregatedFields.add(String.format("%s(%s)", rParts[2].split("=")[1].trim(), field));
        }
      }
    } else { // one aggregated fields
      aggregatedFields.add(new AggregateField(rParts[2].split("=")[1].trim(), parts[0].trim()).toString());
    }

    return Joiner.on(",").join(rParts[0].replaceAll("\\s*\\+\\s*", ","),
        Joiner.on(",").join(aggregatedFields.toArray()));
  }

  @Override
  public AggregationResult xtabs(List<AggregateField> fields) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF groupBy(List<String> groupedColumns, List<String> aggregateFunctions) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double aggregateOnColumn(AggregateFunction function, String col) throws DDFException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public DDF agg(List<String> aggregateFunctions) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF groupBy(List<String> groupedColumns) {
    // TODO Auto-generated method stub
    return null;
  }

}
