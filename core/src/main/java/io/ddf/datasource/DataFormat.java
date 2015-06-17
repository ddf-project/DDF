package io.ddf.datasource;


/**
 */

public enum DataFormat {
  UNDEF(-1), SQL(0), CSV(1), TSV(2), JSON(3), PARQUET(4);

  private int value;

  private DataFormat(int value) {
    this.value = value;
  }
}
