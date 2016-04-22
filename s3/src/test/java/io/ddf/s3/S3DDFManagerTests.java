package io.ddf.s3;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceURI;
import io.ddf.exception.DDFException;


public class S3DDFManagerTests {

    public static S3DDFManager manager;
    public static Logger LOG;

    @Test
    public void testDDFConfig() throws Exception {
        Assert.assertEquals("s3", manager.getEngine());
    }

    @BeforeClass
    public static void startServer() throws Exception {
        LOG = LoggerFactory.getLogger(S3DDFManagerTests.class);
        S3DataSourceDescriptor  s3dsd = new S3DataSourceDescriptor(new S3DataSourceURI(""),
            new S3DataSourceCredentials(System.getenv("AWS_ACCESS_KEY_ID"),
                System.getenv("AWS_SECRET_ACCESS_KEY")),
            null,
            null);
        manager = (S3DDFManager)DDFManager.get(DDFManager.EngineType.S3, s3dsd);

        try {
            DDFManager.get(DDFManager.EngineType.S3,
                new S3DataSourceDescriptor(
                    new S3DataSourceURI(""),
                    new S3DataSourceCredentials("invalid", "invalid"),
                    null,
                    null));
            assert (false);
        } catch (Exception e) {}

        try {
            DDFManager.get(DDFManager.EngineType.S3,
                new S3DataSourceDescriptor(
                    new S3DataSourceURI(""),
                    new S3DataSourceCredentials(System.getenv("AWS_ACCESS_KEY_ID"), "invalid"),
                    null,
                    null
                ));
            assert (false);
        } catch (Exception e) {}
    }

    @Test
    public void testListing() throws DDFException {
        List<String> buckets = manager.listBuckets();
        LOG.info("========== buckets ==========");
        assert(buckets.contains("jing-bucket"));

        LOG.info("========== jing-bucket/testFolder/ ==========");
        List<String> keys = manager.listFiles("jing-bucket/testFolder/");
        assert (keys.size() > 1);
        assert (keys.contains("testFolder/(-_*')!.@&:,$=+?;#.csv"));

        LOG.info("========== jing-bucket/testFolder/a.json ==========");
        keys = manager.listFiles("jing-bucket", "testFolder/a.json");
        assert (keys.size()==1);

        keys = manager.listFiles("adatao-sample-data/test/extra/format/mixed-csv-tsv");
        assert keys.size() > 1;

        keys = manager.listFiles("jing-bucket", "testFolder/(-_*')!.@&:,$=+?;#.csv");
        assert (keys.size() == 1);

        try {
            keys = manager.listFiles("jing-bucket", "non-exist");
            assert (false);
        } catch (DDFException e) {
            assert (e.getMessage().equals("java.io.FileNotFoundException: File does not exist"));
        }
    }

    @Test
    public void testCreateDDF() throws DDFException {
        try {
            S3DDF folderDDF = manager.newDDF("jing-bucket", "testFolder/", null, null);
            assert (false);
            assert(folderDDF.getIsDir() == true);
        } catch (Exception e) {

        }

        try {
            S3DDF mixed = manager.newDDF("adatao-sample-data/test/extra/format/mixed-csv-tsv", null, null);
            assert (false);
        } catch (DDFException e) {
            assert e.getMessage().contains("more than 1");
        }


        S3DDF cleanFolderDDF = manager.newDDF("jing-bucket", "testFolder/folder/", null, null);
        S3DDF jsonDDF = manager.newDDF("jing-bucket", "testFolder/a.json", null, null);
        S3DDF csvDDF = manager.newDDF("jing-bucket", "testFolder/year.csv", null, null);
        assert (cleanFolderDDF.getIsDir() == true);
        assert(jsonDDF.getIsDir() == false);
        assert(csvDDF.getIsDir() == false);
        assert(jsonDDF.getDataFormat().equals(DataFormat.JSON));
        assert(csvDDF.getDataFormat().equals(DataFormat.CSV));

        // PQT, AVRO and ORC
        S3DDF pqtDDF = manager.newDDF("adatao-sample-data", "test/parquet/sleep_parquet/", null, null);
        assert (pqtDDF.getIsDir() == true);
        assert (pqtDDF.getDataFormat().equals(DataFormat.PQT));

        Map<String, String> options = new HashMap<>();
        options.put("format", "parquet");
        S3DDF pqtDDFWithOpts = manager.newDDF("adatao-sample-data", "test/parquet/sleep_parquet/", null, options);
        assert (pqtDDFWithOpts.getIsDir() == true);
        assert (pqtDDF.getDataFormat().equals(DataFormat.PQT));


        S3DDF avroDDF = manager.newDDF("adatao-sample-data", "test/avro/single/episodes.avro", null, null);
        assert (avroDDF.getIsDir() == false);
        assert (avroDDF.getDataFormat().equals(DataFormat.AVRO));

        S3DDF orcDDF = manager.newDDF("adatao-test", "orc/", null, null);
        assert (orcDDF.getIsDir() == true);
        assert (orcDDF.getDataFormat().equals(DataFormat.ORC));

        S3DDF noExtensionDDF = manager.newDDF("adatao-sample-data/test/csv/hasHeader");

        assert (noExtensionDDF.getIsDir() == false);
        assert (noExtensionDDF.getDataFormat().equals(DataFormat.CSV));

        S3DDF emptyDDF = manager.newDDF("adatao-sample-data/empty_folder/");
        assert (emptyDDF.getIsDir() == true);
        assert (emptyDDF.getDataFormat().equals(DataFormat.CSV));

        try {
            S3DDF nestedFolderDDF = manager.newDDF("adatao-sample-data/test/csv/");
            assert false;
        } catch (Exception e) {}

        try {
            S3DDF mixedDDF = manager.newDDF("adatao-sample-data/test/extra/format/mixed-csv-tsv/");
        } catch (Exception e) {
            assert (e.getMessage().contains("more than 1"));
        }
    }


    @Test
    public void testHead() throws DDFException {
        S3DDF csvDDF = manager.newDDF("jing-bucket", "testFolder/year.csv", null, null);
        List<String> rows = manager.head(csvDDF, 5);
        assert(rows.size() == 2);
        LOG.info("========== content of year.csv ==========");
        for (String s : rows) {
            LOG.info(s);
        }

        rows = manager.head(csvDDF, 20000);
        assert(rows.size() == 2);

        rows = manager.head(csvDDF, 1000);
        assert(rows.size() == 2);

        S3DDF folderDDF = manager.newDDF("jing-bucket", "testFolder/folder/", null, null);
        rows = manager.head(folderDDF, 5);
        assert (rows.size() == 4);

        S3DDF ddf1024 = manager.newDDF("jing-bucket", "testFolder/1024.csv", null, null);
        rows = manager.head(ddf1024, 9999);
        assert (rows.size() == 1000);

    }
}
