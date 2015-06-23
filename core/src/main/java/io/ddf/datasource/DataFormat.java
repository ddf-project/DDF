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

  public static DataFormat fromString(String x) {
    if (x.equalsIgnoreCase("SQL")) {
      return SQL;
    } else if (x.equalsIgnoreCase("CSV")) {
      return CSV;
    } else if (x.equalsIgnoreCase("TSV")) {
      return TSV;
    } else if (x.equalsIgnoreCase("JSON")) {
      return JSON;
    } else if (x.equalsIgnoreCase("PQT") || x.equalsIgnoreCase("parquet")) {
      return PQT;
    } else {
      return UNDEF;
    }
  }
}
