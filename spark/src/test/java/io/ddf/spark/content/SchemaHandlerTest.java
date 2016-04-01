package io.ddf.spark.content;

import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import io.ddf.spark.SparkDDF;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import static org.junit.Assert.assertEquals;

/**
 * Created by sangdn on 27/03/2016.
 */
public class SchemaHandlerTest extends BaseTest {

  /**
   * Create table with some type for dataingestion
   * @throws DDFException
   */
  @BeforeClass
  public static void beforeClass() throws DDFException {


    manager.sql("drop table if exists DataIngestion", "SparkSQL");

    manager.sql("create table DataIngestion (type_string string,type_boolean string,type_short string, type_long string,"
                                      + "type_double string,type_decimal string,type_date string,type_timestamp string) "
                                      + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

    manager.sql("load data local inpath '../resources/test/data4ingestion.csv' into table DataIngestion", "SparkSQL");
  }


  @Test
  public void testApplySchema() throws DDFException {

    SparkDDF ddf = (SparkDDF) manager.sql2ddf("select * from DataIngestion", "SparkSQL");
    SchemaHandler schemaHandler = (SchemaHandler) ddf.getSchemaHandler();
    List<Schema.Column> applyColumns = new ArrayList<>();
    applyColumns.add(new Schema.Column("type_string", Schema.ColumnType.STRING));
    applyColumns.add(new Schema.Column("type_boolean", Schema.ColumnType.BOOLEAN));
    applyColumns.add(new Schema.Column("type_short", Schema.ColumnType.SMALLINT));
    applyColumns.add(new Schema.Column("type_long", Schema.ColumnType.BIGINT));
    applyColumns.add(new Schema.Column("type_double", Schema.ColumnType.DOUBLE));
    applyColumns.add(new Schema.Column("type_decimal", Schema.ColumnType.DECIMAL));
    applyColumns.add(new Schema.Column("type_date", Schema.ColumnType.DATE));
    applyColumns.add(new Schema.Column("type_timestamp", Schema.ColumnType.TIMESTAMP));

    Schema applySchema = new Schema(applyColumns);

    testApplyWithDrop(ddf,applySchema);
    testApplyWithoutDrop(ddf,applySchema);



  }

  private void testApplyWithoutDrop(SparkDDF ddf, Schema schema) throws DDFException {
    SchemaHandler schemaHandler = (SchemaHandler) ddf.getSchemaHandler();
    Tuple2<SparkDDF, SchemaHandler.ApplySchemaStatistic> applySchema = schemaHandler.applySchema(schema,false);
    SparkDDF sparkDDF = applySchema._1();
    SchemaHandler.ApplySchemaStatistic statistic = applySchema._2();
    statistic.getMapColumnStatistic().forEach( (colName,colStats) -> {
      System.out.println("column: " + colName);
      System.out.println("numFailed: " + colStats.getNumConvertedFailed());
      System.out.println("num sample" + colStats.getNumSampleData());
      System.out.println("sample" + Arrays.toString(colStats.getSampleData().toArray()));
    });
    assertEquals((long)statistic.getTotalLineProcessed(),2L);
    assertEquals((long)statistic.getTotalLineSuccessed(),2L);
    List<Schema.Column> newColumn = sparkDDF.getSchema().getColumns();
    assertEqualColumn(schema.getColumns(),newColumn);

  }

  private void testApplyWithDrop(SparkDDF ddf,Schema schema) throws DDFException {

    SchemaHandler schemaHandler = (SchemaHandler) ddf.getSchemaHandler();
    Tuple2<SparkDDF, SchemaHandler.ApplySchemaStatistic> applySchema = schemaHandler.applySchema(schema,true);
    SparkDDF sparkDDF = applySchema._1();
    SchemaHandler.ApplySchemaStatistic statistic = applySchema._2();
    statistic.getMapColumnStatistic().forEach( (colName,colStats) -> {
      System.out.println("column: " + colName);
      System.out.println("numFailed: " + colStats.getNumConvertedFailed());
      System.out.println("num sample" + colStats.getNumSampleData());
      System.out.println("sample" + Arrays.toString(colStats.getSampleData().toArray()));
    });
    assertEquals((long)statistic.getTotalLineProcessed(),2L);
    assertEquals((long)statistic.getTotalLineSuccessed(),1L);
    List<Schema.Column> newColumn = sparkDDF.getSchema().getColumns();
    assertEqualColumn(schema.getColumns(),newColumn);
  }
  private void assertEqualColumn(List<Schema.Column> expectedColumn, List<Schema.Column> actualColumns){
    assertEquals(actualColumns.size(), expectedColumn.size());
    for(int i=0; i < actualColumns.size();++i){
      assertEquals(expectedColumn.get(i).getName(),actualColumns.get(i).getName());
      assertEquals(expectedColumn.get(i).getType(),actualColumns.get(i).getType());
      System.out.println(actualColumns.get(i).getName()+ " : " +actualColumns.get(i).getType());
    }
  }

}
