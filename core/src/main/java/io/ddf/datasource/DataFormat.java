package io.ddf.datasource;

public enum DataFormat {
  UNDEF, SQL, CSV, TSV, JSON, PQT;

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
        return PQT;
      default:
        return UNDEF;
    }
  }
}
