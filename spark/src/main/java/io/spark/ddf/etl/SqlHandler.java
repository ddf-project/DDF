/**
 *
 */
package io.spark.ddf.etl;


import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.spark.rdd.RDD;
import scala.Option;
import scala.collection.Seq;
import shark.SharkContext;
import shark.SharkEnv;
import shark.api.Row;
import shark.api.TableRDD;
import shark.memstore2.MemoryTable;
import shark.memstore2.TablePartition;
import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.Schema.DataFormat;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;
import io.spark.ddf.SparkDDF;
import io.spark.ddf.SparkDDFManager;
import io.spark.ddf.content.SchemaHandler;
import org.apache.hadoop.hive.ql.metadata.Hive;

/**
 * @author ctn
 */
public class SqlHandler extends ASqlHandler {

  public SqlHandler(DDF theDDF) {
    super(theDDF);
  }

  private SharkContext getSharkContext() {
    return ((SparkDDFManager) this.getManager()).getSharkContext();
  }

  // ////// IHandleDataCommands ////////

  protected void createTableForDDF(String tableName) throws DDFException {

    try {
      mLog.info(">>>>>> get table for ddf");
      DDF ddf = this.getManager().getDDF(tableName);
      Object rdd = ddf.getRepresentationHandler().get(RDD.class, TablePartition.class);
      if (rdd == null) {
        throw new DDFException("Error getting RDD[TablePartition] representation");
      }
    } catch (Exception e) {
      throw new DDFException(String.format("Can not create table for DDF %s", tableName), e);
    }
  }

  @Override
  public DDF sql2ddf(String command) throws DDFException {
    return this.sql2ddf(command, null, null, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.sql2ddf(command, schema, null, null);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    TableRDD tableRdd = null;
    RDD<Row> rddRow = null;
    // TODO: handle other dataSources and dataFormats

    String tableName = this.getDDF().getSchemaHandler().newTableName();

    /**
     * Make sure that the ddf needed was backed by a table Find tableName that match the pattern
     * "... from tableName ..."
     */
    String ddfTableNameFromQuery;
    Pattern p = Pattern.compile("(?<=.from\\s)([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(command);
    mLog.info(">>>>>>> DDF's TableName from query command = " + command);
    if (m.find()) {
      ddfTableNameFromQuery = m.group();
      mLog.info(">>>>>>> DDF's TableName from query = " + ddfTableNameFromQuery);
      if (this.getManager().getDDF(ddfTableNameFromQuery) != null) {
        this.createTableForDDF(ddfTableNameFromQuery);
      }
    }

    if (dataSource == null) {
      String sqlCmd;

      sqlCmd = String.format("CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"MEMORY_ONLY\") AS %s", tableName,
          command);
      this.getManager().sql2txt(sqlCmd);

      tableRdd = this.getSharkContext().sql2rdd(String.format("SELECT * FROM %s", tableName));
      rddRow = (RDD<Row>) tableRdd;

    } 

    if (schema == null) schema = SchemaHandler.getSchemaFrom(tableRdd.schema());
    /*
     * String tableName = (schema != null ? schema.getTableName() : null);
     * 
     * if (Strings.isNullOrEmpty(tableName)) tableName = (rdd != null ? rdd.name() : null); if
     * (Strings.isNullOrEmpty(tableName)) tableName = this.getDDF().getSchemaHandler().newTableName(); i
     */
    if (tableName != null) {
      schema.setTableName(tableName);
    }
    DDF ddf = this.getManager().newDDF(this.getManager(), rddRow, new Class<?>[] { RDD.class, Row.class }, null,
        tableName, schema);

    try {
      String databaseName = Hive.get(SharkContext.hiveconf()).getCurrentDatabase();
      Option memTableOrNull = SharkEnv.memoryMetadataManager().getMemoryTable(databaseName, tableName);
      if (memTableOrNull.isDefined()) {
        RDD<TablePartition> rddTablePartition = (RDD<TablePartition>) ((MemoryTable) memTableOrNull.get()).getRDD()
            .get();
        ddf.getRepresentationHandler().add(rddTablePartition, RDD.class, TablePartition.class);
      }
    } catch (HiveException e) {
      throw new DDFException(e);
    }

    return ddf;
  }

  private <T> List<T> toList(Seq<T> sequence) {
    return scala.collection.JavaConversions.seqAsJavaList(sequence);
  }


  public static final int MAX_COMMAND_RESULT_ROWS = 1000;


  @Override
  public List<String> sql2txt(String command) throws DDFException {
    return this.sql2txt(command, null, null);
  }

  @Override
  public List<String> sql2txt(String command, Integer maxRows) throws DDFException {
    return this.sql2txt(command, maxRows, null);
  }

  @Override
  public List<String> sql2txt(String command, Integer maxRows, String dataSource) throws DDFException {
    // TODO: handle other dataSources
    /**
     * Make sure that the ddf needed was backed by a table Find tableName that match the pattern
     * "... from tableName ..."
     */
    String ddfTableNameFromQuery;
    Pattern p = Pattern.compile("(?<=.from\\s)([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(command);
    mLog.info(">>>>>>> DDF's TableName from query command = " + command);
    if (m.find()) {
      ddfTableNameFromQuery = m.group();
      mLog.info(">>>>>>> DDF's TableName from query = " + ddfTableNameFromQuery);
      if (this.getManager().getDDF(ddfTableNameFromQuery) != null) {
        this.createTableForDDF(ddfTableNameFromQuery);
      }
    }
    return this.toList(getSharkContext().sql(command, maxRows == null ? MAX_COMMAND_RESULT_ROWS : maxRows));
  }
}
