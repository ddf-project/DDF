/**
 *
 */
package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.TableNameReplacer;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.JDBCDataSourceDescriptor;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.show.ShowTables;

import java.awt.*;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 */
public abstract class ASqlHandler extends ADDFFunctionalGroupHandler implements IHandleSql {

  public ASqlHandler(DDF theDDF) {
    super(theDDF);
  }

  /**
   * @brief Show tables in the database.
   * @return The table names.
   */
  public SqlResult showTables() {
    List<String> tableNames = new ArrayList<String>();
    for (DDF ddf : this.getManager().listDDFs()) {
      if (ddf.getName() != null) {
        tableNames.add(ddf.getName());
      }
    }
    List<Column> columnList = new ArrayList<Column>();
    columnList.add(new Column("table_name", Schema.ColumnType.STRING));
    Schema schema = new Schema("tables", columnList);
    return new SqlResult(schema, tableNames);
  }

  /**
   * @brief Get the column information of this table.
   * @param uri The URI of the ddf.
   * @return The column information.
   * @throws DDFException
   */
  private SqlResult describeTable(String uri) throws DDFException {
    DDF ddf = this.getManager().getDDFByURI(uri);
    if (null == ddf) {
      throw new DDFException("ERROR: there is no ddf with uri " + uri);
    }
    int colSize = ddf.getNumColumns();
    List<String> ret = new ArrayList<String>();
    for (int colIdx = 0; colIdx < colSize; ++colIdx) {
      Schema.Column col = ddf.getColumn(ddf.getColumnName(colIdx));
      ret.add(col.getName().concat("\t").concat(col.getType().toString()));
    }
    List<Column> columnList = new ArrayList<Column>();
    columnList.add(new Column("column_name", Schema.ColumnType.STRING));
    columnList.add(new Column("value_type", Schema.ColumnType.STRING));
    Schema schema = new Schema("table_info", columnList);
    return new SqlResult(schema, ret);
  }

  public SqlResult sqlHandle(String command,
                             Integer maxRows,
                             DataSourceDescriptor dataSource) throws DDFException {
    return this.sqlHandle(command,
                          maxRows,
                          dataSource,
                          new TableNameReplacer(this.getManager()));
  }

  public SqlResult sqlHandle(String command,
                             Integer maxRows,
                             DataSourceDescriptor dataSource,
                             String namespace) throws DDFException {
    return this.sqlHandle(command,
                          maxRows,
                          dataSource,
                          new TableNameReplacer(this.getManager(), namespace));
  }

  public SqlResult sqlHandle(String command,
                             Integer maxRows,
                             DataSourceDescriptor dataSource,
                             List<String> uriList) throws DDFException {
    return this.sqlHandle(command,
                          maxRows,
                          dataSource,
                          new TableNameReplacer(this.getManager(), uriList));
  }


  public SqlResult sqlHandle(String command,
                             Integer maxRows,
                             DataSourceDescriptor dataSource,
                             UUID[] uuidList) throws DDFException {
    return this.sqlHandle(command,
                          maxRows,
                          dataSource,
                          new TableNameReplacer(this.getManager(), uuidList));
  }

  public SqlResult sqlHandle(String sqlcmd,
                             Integer maxRows,
                             DataSourceDescriptor dataSource,
                             TableNameReplacer tableNameReplacer) throws DDFException {
    // If the user specifies the datasource, we should directly send the sql
    // command to the sql engine.
    if (dataSource != null) {
        // TODO: add support for other datasource.
        if (dataSource instanceof JDBCDataSourceDescriptor) {
            // It's the jdbc datasource.
            return this.sql(sqlcmd, maxRows, dataSource);
        }
        SQLDataSourceDescriptor sqlDataSourceDescriptor = (SQLDataSourceDescriptor)dataSource;
        if (sqlDataSourceDescriptor == null) {
            throw  new DDFException("ERROR: Handling datasource");
        }
        if (sqlDataSourceDescriptor.getDataSource() != null) {
            switch (sqlDataSourceDescriptor.getDataSource()) {
                case "SparkSQL":case "spark":case "Spark":
                    return this.sql(sqlcmd, maxRows, dataSource);
                default:
                    throw new DDFException("ERROR: Unrecognized datasource");
            }
        }
    }
    CCJSqlParserManager parserManager = new CCJSqlParserManager();
    StringReader reader = new StringReader(sqlcmd);
    try {
      Statement statement = parserManager.parse(reader);
      if (statement instanceof ShowTables) {
        return this.showTables();
      } else if (statement instanceof  DescribeTable){
        return this.describeTable(((DescribeTable)statement).getName().getName());
      } else if (statement instanceof  Select) {
        // Standard SQL.
        statement = tableNameReplacer.run(statement);
        return this.sql(statement.toString(), maxRows, dataSource);
      } else if (statement instanceof Drop) {
          // TODO: +rename
          return null;
      } else {
          throw  new DDFException("ERROR: Only show tables, describe tables, " +
                  "select, drop, and rename operations are allowed on ddf");
      }
    } catch (Exception e) {
      // It's neither standard SQL nor allowed DDL.
      // e.printStackTrace();
      // Just pass it to lower level SE.
      // return this.sql(sqlcmd, maxRows, dataSource);
      // System.out.println(sqlcmd);
      throw  new DDFException("Please check the syntax. If the query should " +
              "be directly run on engine table, " +
              "please specify dataSource");
    }
  }


  public DDF sql2ddfHandle(String command,
                           Schema schema,
                           DataSourceDescriptor dataSource,
                           DataFormat dataFormat) throws DDFException {
    return sql2ddfHandle(command,
                         schema,
                         dataSource,
                         dataFormat,
                         new TableNameReplacer(this.getManager()));
  }
  public DDF sql2ddfHandle(String command,
                           Schema schema,
                           DataSourceDescriptor dataSource,
                           DataFormat dataFormat,
                           String namespace) throws DDFException {
    return sql2ddfHandle(command,
                         schema,
                         dataSource,
                         dataFormat,
                         new TableNameReplacer(getManager(), namespace));
  }

  public DDF sql2ddfHandle(String command,
                           Schema schema,
                           DataSourceDescriptor dataSource,
                           DataFormat dataFormat,
                           List<String> uriList) throws DDFException {
    return sql2ddfHandle(command,
                         schema,
                         dataSource,
                         dataFormat,
                         new TableNameReplacer(getManager(), uriList));
  }

  public DDF sql2ddfHandle(String command,
                           Schema schema,
                           DataSourceDescriptor dataSource,
                           DataFormat dataFormat,
                           UUID[] uuidList) throws DDFException {
      return sql2ddfHandle(command,
                           schema,
                           dataSource,
                           dataFormat,
                           new TableNameReplacer(getManager(), uuidList));
  }

  public DDF sql2ddfHandle(String command,
                           Schema schema,
                           DataSourceDescriptor dataSource,
                           DataFormat dataFormat,
                           TableNameReplacer tableNameReplacer) throws DDFException {
    if (dataSource != null) {
        if (dataSource instanceof JDBCDataSourceDescriptor) {
            return this.sql2ddf(command, schema, dataSource, dataFormat);
        }
        SQLDataSourceDescriptor sqlDataSourceDescriptor = (SQLDataSourceDescriptor)dataSource;
        if (sqlDataSourceDescriptor == null) {
            throw  new DDFException("ERROR: Handling datasource");
        }
        if (sqlDataSourceDescriptor.getDataSource() != null) {
            switch (sqlDataSourceDescriptor.getDataSource()) {
                case "SparkSQL":case "spark":case "Spark":
                    return this.sql2ddf(command, schema, dataSource, dataFormat);
                default:
                    throw new DDFException("ERROR: Unrecognized datasource: " + dataSource);
            }
        }
    }
    CCJSqlParserManager parserManager = new CCJSqlParserManager();
    StringReader reader = new StringReader(command);
    try {
      Statement statement = parserManager.parse(reader);
      if (!(statement instanceof Select)) {
        throw  new DDFException("ERROR: Only select is allowed in this sql2ddf");
      } else {
        statement = tableNameReplacer.run(statement);
        return this.sql2ddf(statement.toString(), schema, dataSource, dataFormat);
      }
    } catch (Exception e) {
      // It's neither standard SQL nor allowed DDL.
      // e.printStackTrace();
      // Just pass it to lower level SE.
      // System.out.println(command);
      throw new DDFException("Please check the syntax. If the query should " +
              "be directly run on engine table, " +
              "please specify dataSource");
    }
  }

}
