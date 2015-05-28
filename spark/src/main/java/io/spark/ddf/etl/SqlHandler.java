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
import io.spark.ddf.util.SparkUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.codehaus.jackson.JsonGenerator;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.CharArrayWriter;
import com.fasterxml.jackson.core.JsonFactory;
import org.apache.spark.sql.json.JsonRDD;
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
    DataFrame rdd = ((SparkDDFManager) this.getManager()).getHiveContext().sql(command);

    /*
    Row[] arrRow = rdd.collect();
    List<String> lsString = new ArrayList<String>();

    for (Row row : arrRow) {
      lsString.add(row.mkString("\t"));
    }
    return lsString;
    */

    String[] strResult = SparkUtils.jsonForComplexType(rdd, "\t");
    return Arrays.asList(strResult);

  }
}
