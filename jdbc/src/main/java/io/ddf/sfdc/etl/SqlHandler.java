package io.ddf.sfdc.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.jdbc.JDBCDDFManager;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by freeman on 7/15/15.
 */
public class SqlHandler extends io.ddf.jdbc.etl.SqlHandler {

  public SqlHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  public SqlResult sql(String command, Integer maxRows,
                       DataSourceDescriptor dataSource) throws DDFException {
    if (command.toLowerCase().trim().equals("show tables")) {
      return this.showSFDCTables();
    } else {
      return super.sql(command, maxRows, dataSource);
    }
  }

  public SqlResult showSFDCTables(){
    try {
      List<String> tbList = ((JDBCDDFManager)this.getManager()).showTables();
      List<Schema.Column> columnList = new ArrayList<Schema.Column>();
      columnList.add(new Schema.Column("table_name", Schema.ColumnType.STRING));
      Schema schema = new Schema("tables", columnList);
      return new SqlResult(schema, tbList);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    // TODO: what should we return here.
    return new SqlResult(null, new ArrayList<String>());
  }

}
