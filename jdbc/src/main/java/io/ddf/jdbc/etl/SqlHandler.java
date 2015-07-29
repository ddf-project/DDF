package io.ddf.jdbc.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.content.SqlTypedResult;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.JDBCDDF;
import io.ddf.jdbc.JDBCDDFManager;
import io.ddf.jdbc.JDBCUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by freeman on 7/15/15.
 */
public class SqlHandler extends ASqlHandler {

  static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random rnd = new Random();

  String randomString( int len )
  {
    StringBuilder sb = new StringBuilder( len );
    for( int i = 0; i < len; i++ )
      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
    return sb.toString();
  }

  public SqlHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override public DDF sql2ddf(String command) throws DDFException {
    return this.sql2ddf(command, null, null, null);
  }

  @Override public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.sql2ddf(command, schema, null, null);
  }

  @Override public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataSourceDescriptor dataSource) throws DDFException {
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataSourceDescriptor dataSource, DataFormat dataFormat)
      throws DDFException {
    // TODO: We can easily run sql, but we should generate ddf from the SqlResult.
    this.getManager().log("sql2ddf in jdbc");
    Connection conn = this.getConn();
    try {
      Statement statement = conn.createStatement();
      String randomTbName = this.randomString(24);
      this.getManager().log("create table " + randomTbName);
      statement.execute("create table "+ randomTbName + " as (" + command +
              ")");
      DDF ddf = new JDBCDDF((JDBCDDFManager)this.getManager(), null, null, null,
             randomTbName);
      this.getManager().addDDF(ddf);
      this.getManager().log("add ddf");
      return ddf;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new DDFException("Unable to generate jdbc ddf " + e.getMessage());
    }
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
      for (int colIdx = 1; colIdx <= colSize; ++colIdx) {
        columnList.add(new Schema.Column(rsmd.getColumnName(colIdx),
            JDBCUtils.getDDFType(rsmd.getColumnType(colIdx))));
      }

      // TODO: check the table name please.
      // Generate the schema;
      Schema schema = new Schema(rsmd.getTableName(1), columnList);
      List<String> result = new ArrayList<String>();
      StringBuilder sb = new StringBuilder();

      while (rs.next()) {
        //sb.append(rs.getObject(1).toString());
        for (int colIdx = 1; colIdx <= colSize; ++colIdx) {
          Object obj = rs.getObject(colIdx);
          if (obj != null) {
            sb.append(obj.toString());
          } else {
            sb.append("null"); //TODO: how to append NULL
          }
          if (colIdx < colSize) {
            sb.append("\t");
          }
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
