package io.ddf.spark;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceURI;
import io.ddf.exception.DDFException;
import io.ddf.s3.S3DDF;
import io.ddf.s3.S3DDFManager;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class SparkDDFManagerTests extends BaseTest {

  @Test
  public void testDDFConfig() throws Exception {

    Assert.assertEquals("spark", manager.getEngine());
  }

  @Test
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    Map<String, String> params = ((SparkDDFManager) manager).getSparkContextParams();
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));
  }

  @Test
  public void testSimpleSparkDDFManager() throws DDFException {

    createTableAirline();

    List<String> l = manager.sql("select * from airline", "SparkSQL").getRows();
    Assert.assertEquals(31, l.size());

    List<String> v = manager.sql("select count(*) from airline", "SparkSQL").getRows();
    Assert.assertEquals(1, v.size());
    Assert.assertEquals("31", v.get(0));

    DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
        + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    Assert.assertEquals(14, ddf.getSummary().length);
    manager.setDDFName(ddf, "myddf");
    Assert.assertEquals("ddf://adatao/" + ddf.getName(), ddf.getUri());

    manager.addDDF(ddf);
    Assert.assertEquals(ddf, manager.getDDF(ddf.getUUID()));
  }

  @Test
  public void testCopyFromS3() throws DDFException {
    LOG = LoggerFactory.getLogger(SparkDDFManagerTests.class);
    S3DataSourceDescriptor s3dsd = null;
    try {
      s3dsd = new S3DataSourceDescriptor(new S3DataSourceURI(""),
          new S3DataSourceCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")),
          null,
          null);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    S3DDFManager s3DDFManager= (S3DDFManager) DDFManager.get(DDFManager.EngineType.S3, s3dsd);
    SparkDDFManager sparkDDFManager = (SparkDDFManager) manager;

    // Test copy from a folder, the schema should be given.
    System.out.println("========== testFolder/folder ==========");
    S3DDF folderDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/folder/", "year int, value int");
    DDF folderSparkDDF = sparkDDFManager.copyFrom(folderDDF);
    System.out.println(folderSparkDDF.sql("select * from @this", "error").getRows());

    // Test copy from a folder with all json files.
      System.out.println("========== testFolder/allJson ==========");
      S3DDF allJsonDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/allJson/", null);
      DDF allJsonSparkDDF = sparkDDFManager.copyFrom(allJsonDDF);
      System.out.println(allJsonSparkDDF.sql("select * from @this", "error").getRows());

    // Copy From a json, the schema should already be included in the json.
    System.out.println("========== testFolder/folder/d.json ==========");
    S3DDF jsonDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/d.json", null);
    DDF jsonSparkDDF = sparkDDFManager.copyFrom(jsonDDF);
    System.out.println(jsonSparkDDF.sql("select * from @this", "error").getRows());

    // Copy From a csv, the schema is not given.
    System.out.println("========== testFolder/hasHeader.csv ==========");
    S3DDF csvDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/hasHeader.csv", null);
    csvDDF.setHasHeader(true);
    DDF csvSparkDDF = sparkDDFManager.copyFrom(csvDDF);
    System.out.println(csvSparkDDF.sql("select * from @this", "error").getRows());

    // Copy From a csv, the schema is not given, and has no header.
    System.out.println("========== testFolder/noHeader.csv ==========");
    S3DDF csvDDF2 = s3DDFManager.newDDF("jing-bucket", "testFolder/noHeader.csv", null);
    DDF csvSparkDDF2= sparkDDFManager.copyFrom(csvDDF2);
    System.out.println(csvSparkDDF2.sql("select * from @this", "error").getRows());

    // Copy From a csv, the schema is given and has no header.
    System.out.println("========== testFolder/noHeader.csv ==========");
    S3DDF csvDDF3 = s3DDFManager.newDDF("jing-bucket", "testFolder/noHeader.csv", "year int, val " +
        "string");
    DDF csvSparkDDF3 = sparkDDFManager.copyFrom(csvDDF3);
    System.out.println(csvSparkDDF3.sql("select * from @this", "error").getRows());

  }
}
