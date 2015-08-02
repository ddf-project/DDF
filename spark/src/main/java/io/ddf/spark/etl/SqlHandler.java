/**
 *
 */
package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.datasource.DataFormat;
import io.ddf.content.SqlResult;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;
import io.ddf.spark.SparkDDFManager;
import io.ddf.spark.content.SchemaHandler;
import io.ddf.spark.util.SparkUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.collection.Seq;

import java.util.*;
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
    String stringArray[] = command.split(",| |\\.");
    Map<String, String> ddf2tb = new HashMap<String, String>();
    for (String str : stringArray) {
      String tbNameCandidate = str.trim();
      try {
        DDF ddf = this.getManager().getDDFByName(tbNameCandidate);
        ddf2tb.put(tbNameCandidate, ddf.getTableName());
      } catch (DDFException e) {
        e.printStackTrace();
      }
    }

    List<String> sortedNames = new ArrayList<String>(ddf2tb.keySet());
    Collections.sort(sortedNames, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return (o1.length() < o2.length()) ? 1 : -1;
      }
    });

    for (String str : sortedNames) {
      command = command.replace(str, ddf2tb.get(str));
    }
    
    DataFrame rdd = null;
    // TODO: handle other dataSources and dataFormats
    if (dataSource == null) {
      rdd = this.getHiveContext().sql(command);
    } else {
      // TODO
    }
    if (schema == null) schema = SchemaHandler.getSchemaFromDataFrame(rdd);
    DDF ddf = this.getManager().newDDF(this.getManager(), rdd, new Class<?>[] {DataFrame.class}, null,
        null, schema);
    ddf.getRepresentationHandler().get(new Class<?>[]{RDD.class, Row.class});
    return ddf;
  }

  private <T> List<T> toList(Seq<T> sequence) {
    return scala.collection.JavaConversions.seqAsJavaList(sequence);
  }


  public static final int MAX_COMMAND_RESULT_ROWS = 1000;


  @Override
  public SqlResult sql(String command) throws DDFException {
    return this.sql(command, null, null);
  }

  @Override
  public SqlResult sql(String command, Integer maxRows) throws DDFException {
    return this.sql(command, maxRows, null);
  }

  //TODO SparkSql
  @Override
  public SqlResult sql(String command, Integer maxRows, String dataSource) throws DDFException {
    //System.out.println("run sql: \n" + command);
    DataFrame rdd = ((SparkDDFManager) this.getManager()).getHiveContext().sql(command);
    Schema schema = SparkUtils.schemaFromDataFrame(rdd);

    String[] strResult = SparkUtils.df2txt(rdd, "\t");
    return new SqlResult(schema,Arrays.asList(strResult));
  }
}
