package io.ddf2.analytics;


import java.io.Serializable;

/**
 */
public abstract class SimpleSummary implements Serializable {

  private String mColumnName;

  public String getColumnName() {
    return this.mColumnName;
  }

  public void setColumnName(String colName) {
    this.mColumnName = colName;
  }
}

