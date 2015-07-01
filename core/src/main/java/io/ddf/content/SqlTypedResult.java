package io.ddf.content;


import java.util.List;

/**
 * Created by nhanitvn on 24/06/2015.
 */
public class SqlTypedResult {
  public SqlTypedResult(Schema schema, List<String> rows) {
    this.schema = schema;
    this.rows = rows;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public List<String> getRows() {
    return rows;
  }

  public void setRows(List<String> rows) {
    this.rows = rows;
  }

  private Schema schema;
  private List<String> rows;
}
