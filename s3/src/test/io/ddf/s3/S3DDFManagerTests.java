package io.ddf.s3;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceURI;
import io.ddf.exception.DDFException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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
        S3DataSourceDescriptor s3dsd = new S3DataSourceDescriptor(new S3DataSourceURI(""),
            new S3DataSourceCredentials("AKIAIUS54GGBFCR5BWMQ", "k6YNh5bNT6tTZZCBdO+kA7Kd8vCTrqdWBJazgP6T"),
            null,
            null);
        manager = (S3DDFManager)DDFManager.get(DDFManager.EngineType.S3, s3dsd);
    }

    @Test
    public void testCreateDDF() throws DDFException {
        S3DDF folderDDF = manager.newDDF("jing-bucket", "testFolder/", null);
        S3DDF jsonDDF = manager.newDDF("jing-bucket", "testFolder/a.json", null);
        S3DDF csvDDF = manager.newDDF("jing-bucket", "testFolder/year.csv", null);
        assert(folderDDF.getIsDir() == true);
        assert(jsonDDF.getIsDir() == false);
        assert(csvDDF.getIsDir() == false);
        assert(jsonDDF.getDataFormat().equals(DataFormat.JSON));
        assert(csvDDF.getDataFormat().equals(DataFormat.CSV));
        // TODO: Add pqt
    }


    @Test
    public void testHead() throws DDFException {
        S3DDF csvDDF = manager.newDDF("jing-bucket", "testFolder/year.csv", null);
        List<String> rows = manager.head(csvDDF, 5);
        for (String s : rows) {
            System.out.println(s);
        }
    }
}
