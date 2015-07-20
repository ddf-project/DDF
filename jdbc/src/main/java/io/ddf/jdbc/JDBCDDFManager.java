package io.ddf.jdbc;


import io.ddf.datasource.JDBCDataSourceDescriptor;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import io.ddf.misc.Config.ConfigConstant;

import java.sql.*;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The DDF manager for JDBC connector
 *
 * DDF can be created directly on each table on the JDBC connector. We call these DDFs: <b>direct DDF</b>.
 * <br><br>
 *
 * If <code>ddf.jdbc.autocreate = true</code>, a DDF will be generated automatically for each table,
 * else DDF will be created through <code>createDDF(String tableName)</code>.<br><br>
 *
 * Note that <code>ddf@jdbc</code> still follow the general rules of DDF on using SQLHandler.
 * That is a sql query that does not specify source will by default only apply to DDFs not
 * the underlying JDBC database. <br><br>
 *
 * <i>Creating (temporary) DDFs</i>: to avoid polluting the original database, newly <b>created DDF</b> will be stored in a separate
 * database, which is specified by <code>ddf.jdbc.created.ddf.database</code>. This database is hidden from users of DDF.
 * Note that, from user point of view, the <b>created DDF</b> are the same as the direct DDF in the sense
 * that they can query both of them in the same query (e.g. join, where clause etc.)<br><br>
 *
 *
 */
public class JDBCDDFManager extends DDFManager {

  private JDBCDataSourceDescriptor mJdbcDataSource;
  private Connection conn;

  public JDBCDDFManager(JDBCDataSourceDescriptor jdbcDataSource) throws SQLException, ClassNotFoundException {
    /*
     * Register driver for the JDBC connector
     */
    String driver = System.getProperty(ConfigConstant.JDBC_DRIVER_PROPERTY.toString(),
        ConfigConstant.DEFAULT_JDBC_DRIVER.toString());
    Class.forName(driver);

    mJdbcDataSource = jdbcDataSource;
    conn = DriverManager.getConnection(mJdbcDataSource.getDataSourceUri().toString(),
        mJdbcDataSource.getCredentials().getUserName(),
        mJdbcDataSource.getCredentials().getPassword());
  }

  /**
   * Class representing column metadata of a JDBC source
   * @TODO: refactor to make it reusable on any JDBC connector
   */
  public class JDBCColumnMetaData {
    private String mName;
    private Integer mColType;

    /*
      Since atm the following variables are not used programmatically,
      I keep it as string to avoid multiple type conversions between layers.
       Note: the output from the JDBC connector is string
     */
    private String mIsNullable;
    private String mIsAutoIncrement;
    private String mIsGenerated;

    public JDBCColumnMetaData(String name, Integer colType, String isNullable,
        String isAutoIncrement, String isGenerated) {
      this.mName = name;
      this.mColType = colType;
      this.mIsNullable = isNullable;
      this.mIsAutoIncrement = isAutoIncrement;
      this.mIsGenerated = isGenerated;
    }

    @Override public String toString() {
      return String.format("[name: %s, type: %s, isNullable: %s, isAutoIncrement: %s, isGenerated: %s]",
          mName, mColType, mIsNullable, mIsAutoIncrement, mIsGenerated);
    }
  }

  @Override public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
    throw new DDFException("Load DDF from file is not supported!");
  }

  public DDF createDDF(String tableName) throws DDFException {
    return null;
  }

  @Override public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
    return null;
  }

  @Override public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
    return null;
  }

  /**
   *
   * @param
   * @return
   *
   * @TODO: refactor to make it reusable on any JDBC connector
   */
  public List<String> listTables() throws SQLException {

    //assert(conn != null, "");
    DatabaseMetaData dbMetaData = conn.getMetaData();

    ResultSet tbls = dbMetaData.getTables(null, null, null, null);
    List<String> tableList = new ArrayList<String>();

    while(tbls.next()){ tableList.add(tbls.getString("TABLE_NAME")); }
    return tableList;
  }

  /**
   * @param
   * @param tableName
   * @return
   * @throws SQLException
   * @TODO: refactor to make it reusable on any JDBC connector
   */

  public List<JDBCColumnMetaData> getTableMetaData(String tableName) throws SQLException {
    //assert(conn != null, "");
    DatabaseMetaData dbMetaData = conn.getMetaData();
    //assert(tableName != null, "Table name cannot be null");

    ResultSet tblSchema  = dbMetaData.getColumns(null, null, tableName, null);
    List<JDBCColumnMetaData> tblMetaData = new ArrayList<JDBCColumnMetaData>();

    while(tblSchema.next()) {
      JDBCColumnMetaData colMetaData  = new JDBCColumnMetaData(tblSchema.getString("COLUMN_NAME"),
          tblSchema.getInt("DATA_TYPE"),
          tblSchema.getString("IS_NULLABLE"),
          tblSchema.getString("IS_AUTOINCREMENT"),
          tblSchema.getString("IS_GENERATEDCOLUMN")
      );
      tblMetaData.add(colMetaData);
    }
    return tblMetaData;
  }

  @Override public String getEngine() {
    return ConfigConstant.ENGINE_NAME_JDBC.toString();
  }

  /**
   * @param jdbcDataSource
   * @TODO: refactor to make it reusable on any JDBC connector
   */
  public void setJdbcDataSource(JDBCDataSourceDescriptor jdbcDataSource) {
    this.mJdbcDataSource = jdbcDataSource;
  }

  /**
   * Get metadata of the table <code>tableName</code>
   * @param query SQL query string
   * @param conn Connection object
   * @return an array where each element is a list of every row in a column
   * @TODO: refactor to make it reusable on any JDBC connector
   */
  public void runQuery(String query, Connection conn) throws SQLException  {
    //    Statement stm = conn.createStatement();
    //    ResultSet rs = stm.executeQuery(query);
    //
    //    ResultSetMetaData rsMetaData = rs.getMetaData();

    /*
      Read data into array of list, each list is a column
    */
    //    val colCnt = rsMetaData.getColumnCount();
    //    var colList: Array[ListBuffer[Object]] = new Array[ListBuffer[Object]](colCnt);
    //    while(rs.next()){
    //      for (i <- 0 to colCnt-1){
    //        if (colList(i) == null) {
    //          colList(i) = new ListBuffer[Object]();
    //        }
    //        colList(i).append(rs.getObject(i+1));
    //      }
    //    }
    //    colList
  }

  /*
    Implement sql2ddf and sql
    sql(source) will be implemented in generic sql support already
    same for sql2ddf
   */
}
