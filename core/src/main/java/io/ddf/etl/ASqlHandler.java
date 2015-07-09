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
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.show.ShowTables;
import org.apache.avro.generic.GenericData;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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

  public SqlResult sqlHandle(String command, Integer maxRows, String dataSource) throws DDFException {
    return this.sqlHandle(command, maxRows, dataSource, new TableNameReplacer(this.getManager()));
  }

  public SqlResult sqlHandle(String command, Integer maxRows,
                       String dataSource, String namespace) throws DDFException {
    return this.sqlHandle(command, maxRows, dataSource,
            new TableNameReplacer(this.getManager(), namespace));
  }

  public SqlResult sqlHandle(String command, Integer maxRows,
                       String dataSource, List<String> uriList) throws DDFException {
    return this.sqlHandle(command, maxRows, dataSource,
            new TableNameReplacer(this.getManager(), uriList));
  }

  public SqlResult sqlHandle(String sqlcmd, Integer maxRows, String dataSource,
                       TableNameReplacer tableNameReplacer) throws DDFException {
    if (dataSource != null) {
      // TODO.
      return null;
    }
    CCJSqlParserManager parserManager = new CCJSqlParserManager();
    StringReader reader = new StringReader(sqlcmd);
    try {
      // TODO: Add more permission control here.
      Statement statement = parserManager.parse(reader);
      if (statement instanceof ShowTables) {
        return this.showTables();
      } else if (statement instanceof  DescribeTable){
        return this.describeTable(((DescribeTable)statement).getName().getName());
      } else {
        // Standard SQL.
        tableNameReplacer.run(statement);
        return this.sql(statement.toString(), maxRows, dataSource);
      }
    } catch (JSQLParserException e) {
      // It's neither standard SQL nor allowed DDL.
      // e.printStackTrace();
      // Just pass it to lower level SE.
      return this.sql(sqlcmd, maxRows, dataSource);
    }
  }


  public DDF sql2ddfHandle(String command, Schema schema,
                           String dataSource, DataFormat dataFormat) throws DDFException {
    return sql2ddfHandle(command, schema, dataSource, dataFormat, new TableNameReplacer(this.getManager()));
  }
  public DDF sql2ddfHandle(String command, Schema schema,
                           String dataSource, DataFormat dataFormat, String namespace) throws DDFException {
    return sql2ddfHandle(command, schema, dataSource, dataFormat, new TableNameReplacer(getManager(), namespace));
  }
  public DDF sql2ddfHandle(String command, Schema schema,
                           String dataSource, DataFormat dataFormat, List<String> uriList) throws DDFException {
    return sql2ddfHandle(command, schema, dataSource, dataFormat, new TableNameReplacer(getManager(), uriList));
  }

  public DDF sql2ddfHandle(String command, Schema schema,
                           String dataSource, DataFormat dataFormat,
                           TableNameReplacer tableNameReplacer) throws DDFException {
    if (dataSource != null) {
      // TODO
      return null;
    }
    CCJSqlParserManager parserManager = new CCJSqlParserManager();
    StringReader reader = new StringReader(command);
    try {
      Statement statement = parserManager.parse(reader);
      if (!(statement instanceof Select)) {
        throw  new DDFException("Only select is allowed in this function");
      } else {
        tableNameReplacer.run(statement);
        return this.sql2ddf(statement.toString(), schema, dataSource, dataFormat);
      }
    } catch (JSQLParserException e) {
      // It's neither standard SQL nor allowed DDL.
      // e.printStackTrace();
      // Just pass it to lower level SE.
      return this.sql2ddf(command, schema, dataSource, dataFormat);
    }
  }

}
