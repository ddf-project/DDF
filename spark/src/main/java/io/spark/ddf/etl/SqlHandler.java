/**
 *
 */
package io.spark.ddf.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.Schema.DataFormat;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;
import io.spark.ddf.SparkDDF;
import io.spark.ddf.SparkDDFManager;
import io.spark.ddf.content.SchemaHandler;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 */
public class SqlHandler extends ASqlHandler {

  public SqlHandler(DDF theDDF) {
    super(theDDF);
  }

  //  private SparkContext getarkContext() {
  //    return ((SparkDDFManager) this.getManager()).getSharkContext();
  //  }

  // ////// IHandleDataCommands ////////

  private HiveContext getHiveContext() {
    return ((SparkDDFManager) this.getManager()).getHiveContext();
  }

  protected void createTableForDDF(String tableName) throws DDFException {
    try {
      mLog.info(">>>>>> get table for ddf");
      DDF ddf = this.getManager().getDDF(tableName);
      if (ddf != null) {
        DataFrame rdd = (DataFrame) ddf.getRepresentationHandler().get(DataFrame.class);
        if (rdd == null) {
          throw new DDFException("Error getting DataFrame");
        }
        ((SparkDDF) ddf).cacheTable();
      }
    } catch (Exception e) {
      mLog.info(">>>> Exception e.message = " + e.getMessage());
      throw new DDFException(String.format("Can not create table for DDF %s, " + e.getMessage(), tableName), e);
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

  //TODO: SparkSql
  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    //    TableRDD tableRdd = null;
    //    RDD<Row> rddRow = null;
    DataFrame rdd = null;
    // TODO: handle other dataSources and dataFormats

    String tableName = this.getDDF().getSchemaHandler().newTableName();

    /**
     * Make sure that the ddf needed was backed by a table
     * Find tableName that match the pattern
     * "... from tableName ..."
     */
    String ddfTableNameFromQuery;
    Pattern p = Pattern.compile("(?<=.from\\s)([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(command);
    mLog.info(">>>>>>> DDF's TableName from query command = " + command);
    if (m.find()) {
      ddfTableNameFromQuery = m.group();
      mLog.info(">>>>>>> DDF's TableName from query = " + ddfTableNameFromQuery);
      this.createTableForDDF(ddfTableNameFromQuery);
    }

    if (dataSource == null) {

      rdd = this.getHiveContext().sql(command);
    } else {
      // TODO
    }
    if (schema == null) schema = SchemaHandler.getSchemaFromDataFrame(rdd);

    if (tableName != null) {
      schema.setTableName(tableName);
    }

    DDF ddf = this.getManager().newDDF(this.getManager(), rdd, new Class<?>[] {DataFrame.class}, null,
        tableName, schema);
    ((SparkDDF) ddf).cacheTable();

    //get RDD<Row> from the cacheTable DataFrame is faster than recompute from reading from disk.
    //and more memory efficient than cache the original DataFrame
    RDD<Row> rddRow = (this.getHiveContext().sql(String.format("select * from %s", ddf.getTableName()))).rdd();
    ddf.getRepresentationHandler().add(rddRow, RDD.class, Row.class);
    this.getManager().addDDF(ddf);
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

  //TODO SparkSql
  @Override
  public List<String> sql2txt(String command, Integer maxRows, String dataSource) throws DDFException {
    // TODO: handle other dataSources
    /**
     * Make sure that the ddf needed was backed by a table
     * Find tableName that match the pattern
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
    DataFrame rdd = ((SparkDDFManager) this.getManager()).getHiveContext().sql(command);
    Row[] arrRow = rdd.collect();
    List<String> lsString = new ArrayList<String>();

    for (Row row : arrRow) {
      lsString.add(row.mkString("\t"));
    }
    return lsString;
  }
}
