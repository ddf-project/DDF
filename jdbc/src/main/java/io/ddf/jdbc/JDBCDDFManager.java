package io.ddf.jdbc;

import com.google.common.base.Strings;
import io.ddf.content.Schema;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.JDBCDataSourceDescriptor;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import io.ddf.misc.Config.ConfigConstant;
import io.ddf.util.ConfigHandler;
import io.ddf.util.IHandleConfig;

import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The DDF manager for JDBC connector
 *
 * DDF can be created directly on each table on the JDBC connector. We call these DDFs: <b>direct DDF</b>.
 * <br><br>
 *
 * If <code>autocreate = true</code>, a DDF will be generated automatically for each table,
 * else DDF will be created through <code>createDDF(String tableName)</code>.<br><br>
 *
 * Note that <code>ddf@jdbc</code> still follow the general rules of DDF on using SqlHandler.
 * That is a sql query that does not specify source will by default only apply to DDFs not
 * the underlying JDBC database. <br><br>
 *
 * <i>Creating (temporary) DDFs</i>: to avoid polluting the original database, newly <b>created DDF</b> will be stored in a separate
 * database, which is specified by <code>ddf.jdbc.created.ddf.database</code>. This database is hidden from users of DDF.
 * Note that, from user point of view, the <b>created DDF</b> are the same as the direct DDF in the sense
 * that they can query both of them in the same query (e.g. join, where clause etc.)<br><br>
 *
 * TODO (by priority):
 * 1. create direct DDF (automatically or not)
 * 2.
 */
public class JDBCDDFManager extends DDFManager {

  private JDBCDataSourceDescriptor mJdbcDataSource;
  private Connection conn;
  private static IHandleConfig sConfigHandler;


  static {
    String configFileName = System.getenv(ConfigConstant.DDF_INI_ENV_VAR.toString());
    if (Strings.isNullOrEmpty(configFileName)) configFileName = ConfigConstant.DDF_INI_FILE_NAME.toString();
    sConfigHandler = new ConfigHandler(ConfigConstant.DDF_CONFIG_DIR.toString(), configFileName);
  }



  @Override
  public DDF transfer(String fromEngine, String ddfuri) throws DDFException {
    throw new DDFException("Currently jdbc engine doesn't support transfer");
  }

  public JDBCDDFManager()  {
  }

  public JDBCDDFManager(DataSourceDescriptor dataSourceDescriptor, String
          engineType) throws
          Exception {
    /*
     * Register driver for the JDBC connector
     */
    // TODO: check the correctness here.
    super(dataSourceDescriptor);

    this.setEngineType(engineType);
    mLog.info(">>> Initializing JDBCDDFManager");

    String driver = sConfigHandler.getValue(this.getEngine(), ConfigConstant.JDBC_DRIVER.toString());

    Class.forName(driver);

    mJdbcDataSource = (JDBCDataSourceDescriptor) dataSourceDescriptor;

    if (engineType.equals("sfdc")) {
      // Special handler for sfdc connection string, add RTK (for cdata driver)
      URI uriWithRTK = new URI(mJdbcDataSource
              .getDataSourceUri().getUri().toString()
              + "RTK='" + cdata.jdbc.salesforce.SalesforceDriver.getRTK()+"';");
      mJdbcDataSource.getDataSourceUri().setUri(uriWithRTK);
    }

    this.setDataSourceDescriptor(dataSourceDescriptor);
    if (mJdbcDataSource == null) {
      throw new Exception("JDBCDataSourceDescriptor is null when initializing "
              + "JDBCDDFManager");
    }
    conn = DriverManager.getConnection(mJdbcDataSource.getDataSourceUri().toString(),
        mJdbcDataSource.getCredentials().getUsername(),
        mJdbcDataSource.getCredentials().getPassword());

    mLog.info(">>> Set up connection with jdbc : " + mJdbcDataSource
            .getDataSourceUri().toString());
    boolean isDDFAutoCreate = Boolean.parseBoolean(sConfigHandler.getValue(ConfigConstant.ENGINE_NAME_JDBC.toString(),
        ConfigConstant.JDBC_DDF_AUTOCREATE.toString()));

    this.showTables();
    mLog.info(">>> Connection is set up");
    if (isDDFAutoCreate){

    } else {

    }

  }

  /**
   * @brief Close the jdbc connection.
   * @throws Throwable
   */
  protected void finalize() throws Throwable {
    this.conn.close();
  }

  public JDBCDataSourceDescriptor getJdbcDataSource() {
    return mJdbcDataSource;
  }

  public Connection getConn() {
    return conn;
  }

  public void setConn(Connection conn) {
    this.conn = conn;
  }

  public static IHandleConfig getConfigHandler() {
    return sConfigHandler;
  }

  public static void setConfigHandler(IHandleConfig sConfigHandler) {
    JDBCDDFManager.sConfigHandler = sConfigHandler;
  }

  /**
   * Class representing column metadata of a JDBC source
   * @TODO: refactor to make it reusable on any JDBC connector
   */
  public class ColumnSchema {

    private String name;
    private Integer colType;

    /*
      Since atm the following variables are not used programmatically,
      I keep it as string to avoid multiple type conversions between layers.
       Note: the output from the JDBC connector is string
     */
    private String isNullable;
    private String isAutoIncrement;
    private String isGenerated;

    public ColumnSchema(String name, Integer colType, String isNullable, String isAutoIncrement, String isGenerated) {
      this.name = name;
      this.colType = colType;
      this.isNullable = isNullable;
      this.isAutoIncrement = isAutoIncrement;
      this.isGenerated = isGenerated;
    }

    public ColumnSchema(String name, Integer colType, String isNullable, String isAutoIncrement) {
      this(name, colType, isNullable, isAutoIncrement, null);
    }

    public String getName() {
      return name;
    }

    /**
     * @Getter and Setter.
     * @return
     */

    public Integer getColType() {
      return colType;
    }

    public void setColType(Integer colType) {
      this.colType = colType;
    }

    @Override public String toString() {
      return String.format("[name: %s, type: %s, isNullable: %s, isAutoIncrement: %s, isGenerated: %s]", name, colType,
          isNullable, isAutoIncrement, isGenerated);
    }
  }

  public class TableSchema extends ArrayList<ColumnSchema> {}

  @Override public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
    throw new DDFException("Load DDF from file is not supported!");
  }

  public DDF DDF(String tableName) throws DDFException {
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
  public List<String> showTables() throws SQLException {

    //assert(conn != null, "");
    DatabaseMetaData dbMetaData = conn.getMetaData();

    ResultSet tbls = dbMetaData.getTables(null, null, null, null);
    List<String> tableList = new ArrayList<String>();

    while(tbls.next()){ tableList.add(tbls.getString("TABLE_NAME")); }
    return tableList;
  }

  public List<String> listColumns(String tablename) throws SQLException {
    DatabaseMetaData dbmd = conn.getMetaData();
    ResultSet columns = dbmd.getColumns(null, null, tablename, null);
    List<String> columnNames = new ArrayList<String>();
    while (columns.next()) {
      columnNames.add(columns.getString("COLUMN_NAME"));
    }
    return columnNames;
  }

  /**
   * @param
   * @param tableName
   * @return
   * @throws SQLException
   * @TODO: refactor to make it reusable on any JDBC connector
   */

  public TableSchema getTableSchema(String tableName) throws SQLException {
    //assert(conn != null, "");
    DatabaseMetaData dbMetaData = conn.getMetaData();
    //assert(tableName != null, "Table name cannot be null");

    ResultSet tblSchemaResult  = dbMetaData.getColumns(null, null, tableName, null);
    TableSchema tblSchema = new TableSchema();

    while(tblSchemaResult.next()) {
      ColumnSchema colSchema  = new ColumnSchema(tblSchemaResult.getString("COLUMN_NAME"),
          tblSchemaResult.getInt("DATA_TYPE"),
          tblSchemaResult.getString("IS_NULLABLE"),
          tblSchemaResult.getString("IS_AUTOINCREMENT")
          //tblSchemaResult.getString("IS_GENERATEDCOLUMN")
      );
      tblSchema.add(colSchema);
    }
    return tblSchema;
  }

  @Override public String getEngine() {
    // return ConfigConstant.ENGINE_NAME_JDBC.toString();
    return this.getEngineType();
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

}
