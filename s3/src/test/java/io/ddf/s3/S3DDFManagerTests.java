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

    private Map<String, String> withFormat(String format) {
        Map<String, String> options = new HashMap<>();
        options.put("format", format);
        return options;
    }
}
