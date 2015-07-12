package io.ddf.content;


import java.util.ArrayList;
import java.util.List;

/**
 * This class is for the sql result with type of every cell specified.
 */
public class SqlTypedResult {
  // The schema of the result.
  private Schema schema;
  // The table content.
  private List<List<SqlTypedCell>> rows;

  /**
   * @brief Constructor.
   * @param schema The schema.
   * @param rows The content of the table.
   */
  public SqlTypedResult(Schema schema, List<List<SqlTypedCell>> rows) {
    this.schema = schema;
    this.rows = rows;
  }

  /**
   * @brief Construtor
   * @param sqlResult The result that is of SqlResult type.
   */
  public SqlTypedResult(SqlResult sqlResult) {
      Schema schema = sqlResult.getSchema();
      this.schema = schema;
      List<String> nonTypedRows = sqlResult.getRows();
      if (null == nonTypedRows) {
        this.rows = null;
        return;
      }
      int rowSize = nonTypedRows.size();
      int colSize = schema.getNumColumns();
      if (0 == rowSize) {
        this.rows = null;
        return;
      }

      this.rows = new ArrayList<List<SqlTypedCell>>();
      for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
        List<SqlTypedCell> row = new ArrayList<SqlTypedCell>();
        String[] splitArray = nonTypedRows.get(rowIdx).split("\t");
        assert(splitArray.length == schema.getNumColumns());
        for (int colIdx = 0; colIdx < colSize; ++colIdx) {
          row.add(new SqlTypedCell(schema.getColumn(colIdx).getType(), splitArray[colIdx]));
        }
        this.rows.add(row);
      }
  }

  /**
   * @brief Getters and Setters.
   */
  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public List<List<SqlTypedCell>> getRows() {
    return rows;
  }

  public void setRows(List<List<SqlTypedCell>> rows) {
    this.rows = rows;
  }
}
