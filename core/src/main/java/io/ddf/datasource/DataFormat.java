package io.ddf.datasource;

public enum DataFormat {
  UNDEF, SQL, CSV, TSV, JSON, PARQUET, AVRO, ORC;

  public static DataFormat fromInt(int x) {
    switch(x) {
      case 0:
        return SQL;
      case 1:
        return CSV;
      case 2:
        return TSV;
      case 3:
        return JSON;
      case 4:
        return PARQUET;
      case 5:
        return AVRO;
      case 6:
        return ORC;
      default:
        return UNDEF;
    }
  }
}
