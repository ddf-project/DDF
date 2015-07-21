package io.ddf.jdbc.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.content.SqlTypedResult;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.JDBCDDFManager;
import io.ddf.jdbc.JDBCUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by freeman on 7/15/15.
 */
public class SqlHandler extends ASqlHandler {
  public SqlHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override public DDF sql2ddf(String command) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataSourceDescriptor dataSource) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataSourceDescriptor dataSource, DataFormat dataFormat)
      throws DDFException {
    // TODO: We can easily run sql, but we should generate ddf from the SqlResult.
    return null;
  }

  /**
   * @brief Get connection.
   * @return The connection.
   */
  private Connection  getConn() {
    return ((JDBCDDFManager)this.getManager()).getConn();
  }

  @Override
  public SqlResult sql(String command) throws DDFException {
    return this.sql(command, null);
  }

  @Override public SqlResult sql(String command, Integer maxRows) throws DDFException {
    return this.sql(command, null, null);
  }

  //TODO: handle dataSource
  @Override public SqlResult sql(String command, Integer maxRows, DataSourceDescriptor dataSource) throws DDFException {
    Connection conn = this.getConn();
    Statement statement;
    try {
      statement = conn.createStatement();
      ResultSet rs = statement.executeQuery(command);
      ResultSetMetaData rsmd = rs.getMetaData();
      int colSize = rsmd.getColumnCount();
      if (colSize == 0) {
        // TODO: How to return empty results?
        return new SqlResult(null, null);
      }
      List<Schema.Column> columnList = new ArrayList<Schema.Column>();
      for (int colIdx = 1; colIdx < colSize; ++colIdx) {
        columnList.add(new Schema.Column(rsmd.getColumnName(colIdx),
            JDBCUtils.getDDFType(rsmd.getColumnType(colIdx))));
      }

      // TODO: check the table name please.
      // Generate the schema;
      Schema schema = new Schema(rsmd.getTableName(1), columnList);
      List<String> result = new ArrayList<String>();
      StringBuilder sb = new StringBuilder();

      while (rs.next()) {
        sb.append(rs.getObject(1).toString());
        for (int colIdx = 1; colIdx < colSize; ++colIdx) {
          sb.append("\t").append(rs.getObject(colIdx).toString());
        }
        result.add(sb.toString());
        sb.delete(0, sb.length());
      }

      return new SqlResult(schema, result);
    } catch (SQLException e) {
      mLog.debug(e.getMessage());
      e.printStackTrace();
      throw new DDFException("Encouter error when running sql query using jdbc");
    }
  }

  @Override public SqlTypedResult sqlTyped(String command) throws DDFException {
    return null;
  }

  @Override public SqlTypedResult sqlTyped(String command, Integer maxRows) throws DDFException {
    return null;
  }

  @Override public SqlTypedResult sqlTyped(String command, Integer maxRows, DataSourceDescriptor dataSource)
      throws DDFException {
    return null;
  }

}
