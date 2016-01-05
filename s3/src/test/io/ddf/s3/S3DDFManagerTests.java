package io.ddf.s3;

import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceURI;
import io.ddf.exception.DDFException;
import io.ddf.s3.S3DDF;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class S3DDFManagerTests {

    public static S3DDFManager manager;
    public static Logger LOG;

    @Test
    public void testDDFConfig() throws Exception {
        Assert.assertEquals("s3", manager.getEngine());
    }

    @BeforeClass
    public static void startServer() throws Exception {
        Thread.sleep(1000);
        LOG = LoggerFactory.getLogger(S3DDFManagerTests.class);
        S3DataSourceDescriptor  s3dsd = new S3DataSourceDescriptor(new S3DataSourceURI(""),
            new S3DataSourceCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")),
            null,
            null);
        manager = (S3DDFManager)DDFManager.get(DDFManager.EngineType.S3, s3dsd);
    }

    @Test
    public void testListing() throws DDFException {
        List<String> buckets = manager.listBuckets();
        LOG.info("========== buckets ==========");
        for (String bucket: buckets) {
            LOG.info(bucket);
        }
        assert(buckets.contains("jing-bucket"));

        LOG.info("========== jing-bucket/testFolder/ ==========");
        List<String> keys = manager.listFiles("jing-bucket", "testFolder/");
        for (String key: keys) {
            LOG.info(key);
        }
        assert (keys.size()== 21);
        assert (keys.contains("testFolder/(-_*')!.@&:,$=+?;#.csv"));

        LOG.info("========== jing-bucket/testFolder/a.json ==========");
        keys = manager.listFiles("jing-bucket", "testFolder/a.json");
        for (String key: keys) {
            LOG.info(key);
        }
        assert (keys.size()==1);

        keys = manager.listFiles("jing-bucket", "testFolder/(-_*')!.@&:,$=+?;#.csv");
        assert (keys.size() == 1);

    }

    @Test
    public void testCreateDDF() throws DDFException {
        try {
            S3DDF folderDDF = manager.newDDF("jing-bucket", "testFolder/", null);
            assert (false);
            assert(folderDDF.getIsDir() == true);
        } catch (Exception e) {

        }

        S3DDF cleanFolderDDF = manager.newDDF("jing-bucket", "testFolder/folder/", null);
        S3DDF jsonDDF = manager.newDDF("jing-bucket", "testFolder/a.json", null);
        S3DDF csvDDF = manager.newDDF("jing-bucket", "testFolder/year.csv", null);
        assert (cleanFolderDDF.getIsDir() == true);
        assert(jsonDDF.getIsDir() == false);
        assert(csvDDF.getIsDir() == false);
        assert(jsonDDF.getDataFormat().equals(DataFormat.JSON));
        assert(csvDDF.getDataFormat().equals(DataFormat.CSV));
        try {
            // Test on non-exist folder/file. Should throw exception.
            S3DDF nonExistDDF = manager.newDDF("jing-bucket", "nonexist.csv", null);
            assert (false);
        } catch (Exception e) {

        }
        try {
            S3DDF nonExistDDF2 = manager.newDDF("jing-bucket", "nonexist/", null);
            assert (false);
        } catch (Exception e) {

        }
        // TODO: Add pqt
    }


    @Test
    public void testHead() throws DDFException {
        S3DDF csvDDF = manager.newDDF("jing-bucket", "testFolder/year.csv", null);
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

        S3DDF folderDDF = manager.newDDF("jing-bucket", "testFolder/folder/", null);
        rows = manager.head(folderDDF, 5);
        assert (rows.size() == 4);

        S3DDF ddf1024 = manager.newDDF("jing-bucket", "testFolder/1024.csv", null);
        rows = manager.head(ddf1024, 9999);
        assert (rows.size() == 1000);

    }
}
