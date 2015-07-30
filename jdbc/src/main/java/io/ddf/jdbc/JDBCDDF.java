package io.ddf.jdbc;


import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.facades.MLFacade;
import io.ddf.facades.RFacade;
import io.ddf.facades.TransformFacade;
import io.ddf.facades.ViewsFacade;
import io.ddf.jdbc.JDBCDDFManager.TableSchema;
import io.ddf.jdbc.JDBCDDFManager.ColumnSchema;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by freeman on 7/17/15.
 */
public class JDBCDDF extends DDF {

  /**
   *  Create a DDF from a table
   *
   * @param manager
   * @param namespace
   * @param name: DDF name
   * @param tableName: JDBC table name
   */
  public JDBCDDF(JDBCDDFManager manager, String namespace, String name, String tableName)
      throws DDFException, SQLException {
    //build DDF schema from table schema
    TableSchema tableSchema = manager.getTableSchema(tableName);
    Schema ddfSchema = buildDDFSchema(tableSchema);
    this.initialize(manager, null, null, namespace, name, ddfSchema, tableName);
  }

  /**
   * Signature without RDD, useful for creating a dummy DDF used by DDFManager
   *
   * @param manager
   */
  public JDBCDDF(DDFManager manager) throws DDFException {
    super(manager);
  }
  
  @Override
  public DDF copy() throws DDFException {
    return null;
  }
  private Schema buildDDFSchema(TableSchema tableSchema) throws DDFException {
    List<Schema.Column> cols = new ArrayList<>();
    Iterator<ColumnSchema> schemaIter = tableSchema.iterator();

    while(schemaIter.hasNext()){
      ColumnSchema jdbcColSchema = schemaIter.next();
      Schema.ColumnType colType = JDBCUtils.getDDFType(jdbcColSchema.getColType()); //TODO: verify if throwing exception makes sense
      String colName = jdbcColSchema.getName();
      cols.add(new Schema.Column(colName, colType));
    }
    return new Schema(null, cols);
  }
}
