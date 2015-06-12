package io.ddf.analytics;


import java.util.List;

/**
 */
public class CategoricalSimpleSummary extends SimpleSummary {
  //list of all values in column
  private List<String> mValues;

  public void setValues(List<String> values) {
    this.mValues = values;
  }

  public List<String> getValues() {
    return this.mValues;
  }
}
