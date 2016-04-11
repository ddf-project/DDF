package io.ddf.spark;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceURI;
import io.ddf.exception.DDFException;
import io.ddf.hdfs.HDFSDDF;
import io.ddf.hdfs.HDFSDDFManager;
import io.ddf.s3.S3DDF;
import io.ddf.s3.S3DDFManager;


import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkDDFManagerTests extends BaseTest {

  @Test
  public void testDDFConfig() throws Exception {

    Assert.assertEquals("spark", manager.getEngine());
  }

  @Test
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    Map<String, String> params = ((SparkDDFManager) manager).getSparkContextParams();
    LOG.info(System.getProperty("spark.serializer"));
    LOG.info(params.get("DDFSPARK_JAR"));
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


  public void testBasicCopyForS3(S3DDFManager s3DDFManager) throws DDFException {
    // Test copy from a folder, the schema should be given.
    LOG.info("========== testFolder/folder ==========");
    S3DDF folderDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/folder/", "year int, value int", null);
    DDF folderSparkDDF = manager.copyFrom(folderDDF);
    LOG.info(folderSparkDDF.sql("select * from @this", "error").getRows().toString());
    assert(folderSparkDDF.getNumRows() == 4);

    // Test copy from a folder with all json files.
    LOG.info("========== testFolder/allJson ==========");
    S3DDF allJsonDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/allJson/", null, null);
    DDF allJsonSparkDDF = manager.copyFrom(allJsonDDF);
    // TODO: is it 6?
    // assert(allJsonSparkDDF.getNumRows() == 4);

    LOG.info("========== testFolder/folder/d.json ==========");
    S3DDF jsonDDF = s3DDFManager.newDDF("adatao-test", "json/", null, null);
    DDF jsonSparkDDF = manager.copyFrom(jsonDDF);
    LOG.info(jsonSparkDDF.sql("select * from @this limit 5", "error").getRows().toString());


    // Copy From a csv, the schema is not given.
    LOG.info("========== testFolder/hasHeader.csv ==========");
    Map<String, String> options = new HashMap<>() ;
    options.put("header", "true");
    S3DDF csvDDF = s3DDFManager.newDDF("jing-bucket", "testFolder/hasHeader.csv", null, options);
    DDF csvSparkDDF = manager.copyFrom(csvDDF);
    LOG.info(csvSparkDDF.sql("select * from @this", "error").getRows().toString());
    assert(csvSparkDDF.getNumRows()==2);


    // Copy From a csv, the schema is not given, and has no header.
    LOG.info("========== testFolder/noHeader.csv ==========");
    S3DDF csvDDF2 = s3DDFManager.newDDF("jing-bucket", "testFolder/noHeader.csv", null, null);
    DDF csvSparkDDF2= manager.copyFrom(csvDDF2);
    LOG.info(csvSparkDDF2.sql("select * from @this", "error").getRows().toString());
    assert(csvSparkDDF2.getNumRows()==2);

    // Copy From a csv, the schema is given and has no header.
    LOG.info("========== testFolder/noHeader.csv ==========");
    S3DDF csvDDF3 = s3DDFManager.newDDF("jing-bucket", "testFolder/noHeader.csv", "year int, val string", null);
    DDF csvSparkDDF3 = manager.copyFrom(csvDDF3);
    LOG.info(csvSparkDDF3.sql("select * from @this", "error").getRows().toString());
    assert (csvSparkDDF3.getNumRows()==2);


    LOG.info("========== tsv ==========");
    S3DDF tsvDDF = s3DDFManager.newDDF("adatao-sample-data", "test/tsv/noheader/results.tsv", "ID int, FlagTsunami " +
        "string, Year int, " +
        "Month int, Day int, Hour int, Minute int, Second double, FocalDepth int, EqPrimary double, EqMagMw double, EqMagMs double, EqMagMb double, EqMagMl double, EqMagMfd double, EqMagUnk double, Intensity int, Country string, State string, LocationName string, Latitude double, Longitude double, RegionCode int, Death int, DeathDescription int, Injuries int, InjuriesDescription int", null);
    // TODO: Check the file?
    /*
    DDF tsvSparkDDF = sparkDDFManager.copyFrom(tsvDDF);
    LOG.info(tsvSparkDDF.sql("select * from @this", "error").getRows().toString());
    assert (tsvSparkDDF.getNumRows()> 0);
    */

    LOG.info("========== pqt ==========");
    S3DDF pqtDDF = s3DDFManager.newDDF("adatao-sample-data", "test/parquet/sleep_parquet/", null, null);
    DDF pqtSparkDDF = manager.copyFrom(pqtDDF);
    LOG.info(pqtSparkDDF.sql("select * from @this limit 5", "error").getRows().toString());
    LOG.info("========== avro ==========");
    //S3DDF avroDDF = s3DDFManager.newDDF("adatao-sample-data", "test/avro/partition/", null, null);
    //DDF avroSparkDDF = manager.copyFrom(avroDDF);
    //LOG.info(avroSparkDDF.sql("select * from @this limit 5", "error").getRows().toString());

    // s3 doesn't work with orc now
    /*
    LOG.info("========== orc ==========");
    S3DDF orcDDF = s3DDFManager.newDDF("adatao-sample-data", "test/orc/", null);
    DDF orcSparkDDF = sparkDDFManager.copyFrom(orcDDF);
    LOG.info(orcSparkDDF.sql("select * from @this limit 5", "error").getRows().toString());
    */

    // empty files doesn't work now
    /*
    LOG.info("========= empty files =========");
    S3DDF emptyDDF = s3DDFManager.newDDF("adatao-sample-data", "test_empty_files/",
        "val1 string, val2 string, val3 string, val4 string, val5 string, val6 string," +
            "val7 string, val8 string, val9 string, val10 string, val11 string, val12 string, val13 string", null);
    DDF emptySparkDDF = sparkDDFManager.copyFrom(emptyDDF);
    LOG.info(emptySparkDDF.sql("select * from @this limit 5", "error").getRows().toString());
    */
  }

  public void testOptions(S3DDFManager s3DDFManager) throws DDFException {
    testComment(s3DDFManager);
    testDelim(s3DDFManager);
    testEscape(s3DDFManager);
    testQuote(s3DDFManager);
    testNull(s3DDFManager);
  }

  public void testDelim(S3DDFManager s3DDFManager) throws DDFException {
    final Map<String, String> delim2pathMap = ImmutableMap.<String, String>builder()
        .put(" ", "adatao-sample-data/test/extra/delim/space/")
        .put("|", "adatao-sample-data/test/extra/delim/vertical-bar/")
        .put("\u0001", "adatao-sample-data/test/extra/delim/ctra/")
        .build();
    Map<String, String> options = new HashMap<>();
    for (Map.Entry<String, String> entry: delim2pathMap.entrySet()) {
      options.clear();
      options.put("delimiter", entry.getKey());
      S3DDF s3DDF = s3DDFManager.newDDF(entry.getValue(), options);
      DDF sparkDDF = manager.copyFrom(s3DDF);
      assert (sparkDDF.getNumColumns() == 3);
    }
  }

  public void testQuote(S3DDFManager s3DDFManager) {

  }

  public void testEscape(S3DDFManager s3DDFManager) {

  }

  public void testComment(S3DDFManager s3DDFManager) throws DDFException {}

  public void testNull(S3DDFManager s3DDFManager) throws DDFException {
    final Map<String, String> nullvalue2pathMap = ImmutableMap.<String, String>builder()
        .put("<null>", "adatao-sample-data/test/extra/nullvalue/null-using-null/null-using-flashn")
        .put("/N", "adatao-sample-data/test/extra/nullvalue/null-using-fslashn/null-using-flashn")
        .put("\\n", "adatao-sample-data/test/extra/nullvalue/null-using-newline/null-using-flashn")
        .build();
    Map<String, String> options = new HashMap<>();
    for (Map.Entry<String, String> entry: nullvalue2pathMap.entrySet()) {
      options.clear();
      options.put("nullvalue", entry.getKey());
      S3DDF s3DDF = s3DDFManager.newDDF(entry.getValue(), options);
      DDF sparkDDF = manager.copyFrom(s3DDF);
      assert (sparkDDF.getNumRows() > 0);
    }
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
      throw new DDFException(e);
    }
    S3DDFManager s3DDFManager= (S3DDFManager) DDFManager.get(DDFManager.EngineType.S3, s3dsd);
    testBasicCopyForS3(s3DDFManager);
    testOptions(s3DDFManager);
  }

  @Test
  public void testCopyFromHDFS() throws DDFException {
    LOG = LoggerFactory.getLogger(SparkDDFManagerTests.class);
    HDFSDDFManager hdfsDDFManager = new HDFSDDFManager(System.getenv("HDFS_URI"));
    SparkDDFManager sparkDDFManager = (SparkDDFManager) manager;

    // Test copy from a folder, the schema should be given.
    LOG.info("========== test_pe/csv/small ==========");
    HDFSDDF folderDDF = hdfsDDFManager.newDDF("/test_pe/csv/small/", "year int, value int", null);
    DDF folderSparkDDF = sparkDDFManager.copyFrom(folderDDF);
    LOG.info(folderSparkDDF.sql("select * from @this", "error").getRows().toString());
    assert(folderSparkDDF.getNumRows() == 4);

    // Test copy from a folder with all json files.
    LOG.info("========== test_pe/json/single ==========");
    HDFSDDF jsonDDF = hdfsDDFManager.newDDF("/test_pe/json/single/flat_sleep_data.json", null, null);
    DDF jsonSparkDDF = sparkDDFManager.copyFrom(jsonDDF);
    assert(jsonSparkDDF.getNumRows() > 0);

    // Copy From a json, the schema should already be included in the json.
    /* check the hdfs files
    LOG.info("========== test_pe/json/multiple  ==========");
    HDFSDDF allJsonDDF = hdfsDDFManager.newDDF("/test_pe/json/multiple", null, null);
    DDF allJsonSparkDDF = sparkDDFManager.copyFrom(allJsonDDF);
    LOG.info(allJsonSparkDDF.sql("select * from @this", "error").getRows().toString());
    // assert (allJsonSparkDDF.getNumRows() > 0);
    */


    // Copy From a csv, the schema is not given, and has no header.
    LOG.info("========== test_pe/csv/noheader/crime.csv ==========");
    HDFSDDF csvDDF2 = hdfsDDFManager.newDDF("/test_pe/csv/noheader/crime.csv", null);
    DDF csvSparkDDF2= sparkDDFManager.copyFrom(csvDDF2);
    LOG.info(csvSparkDDF2.sql("select * from @this", "error").getRows().toString());
    assert(csvSparkDDF2.getNumRows()==2);

    // Copy From a csv, the schema is given and has no header.
    LOG.info("========== test_pe/csv/noheader/crime.csv ==========");
    String crimeSchema = "ID int, CaseNumber string, Date string, Block string, IUCR string, PrimaryType string, " +
        "Description string, LocationDescription string, Arrest boolean, Domestic boolean, Beat int, District int, " +
        "Ward int, CommunityArea int, FBICode string, XCoordinate int, YCoordinate int, Year int, UpdatedOn string, " +
        "Latitude double, Longitude double, Location string";
    HDFSDDF csvDDF3 = hdfsDDFManager.newDDF("/test_pe/csv/noheader/crime.csv", crimeSchema, null);
    DDF csvSparkDDF3 = sparkDDFManager.copyFrom(csvDDF3);
    LOG.info(csvSparkDDF3.sql("select * from @this", "error").getRows().toString());
    assert (csvSparkDDF3.getNumRows() > 0);


    LOG.info("========== pqt ==========");
    HDFSDDF pqtDDF = hdfsDDFManager.newDDF("/test_pe/parquet/default", null, null);
    DDF pqtSparkDDF = sparkDDFManager.copyFrom(pqtDDF);
    LOG.info(pqtSparkDDF.sql("select * from @this limit 5", "error").getRows().toString());
    assert (pqtSparkDDF.getNumRows() > 0);

    LOG.info("========== avro ==========");
    HDFSDDF avroDDF = hdfsDDFManager.newDDF("/test_pe/avro/partition", null, null);
    DDF avroSparkDDF = sparkDDFManager.copyFrom(avroDDF);
    LOG.info(avroSparkDDF.sql("select * from @this limit 5", "error").getRows().toString());
    assert (avroSparkDDF.getNumRows() > 0);

    LOG.info("========== orc ==========");
    HDFSDDF orcDDF = hdfsDDFManager.newDDF("/test_pe/orc/default/", null, null);
    DDF orcSparkDDF = sparkDDFManager.copyFrom(orcDDF);
    assert (orcSparkDDF.getNumRows() > 0);
  }

}
