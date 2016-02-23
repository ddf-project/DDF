package io.ddf.hdfs;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;


public class HDFSDDFManagerTests {

    public static HDFSDDFManager manager;
    public static Logger LOG;

    @Test
    public void testDDFConfig() throws Exception {
        Assert.assertEquals("hdfs", manager.getEngine());
    }

    @BeforeClass
    public static void startServer() throws Exception {
        Thread.sleep(1000);
        LOG = LoggerFactory.getLogger(HDFSDDFManagerTests.class);
        manager = (HDFSDDFManager)DDFManager.get(DDFManager.EngineType.HDFS);
    }

    @Test
    public void testListing() throws DDFException {
        List<String> files = manager.listFiles("/user");
        LOG.info("========== buckets ==========");
        for (String file: files) {
            System.out.println(file);
            LOG.info(file);
        }

    }


    @Test
    public void testCreateDDF() throws DDFException {
        try {
            HDFSDDF folderDDF = manager.newDDF("/user/jing/hasHeader.csv", null);
            assert (false);
            assert(folderDDF.getIsDir() == true);
        } catch (Exception e) {

        }

        HDFSDDF cleanFolderDDF = manager.newDDF("", null);
        HDFSDDF jsonDDF = manager.newDDF("", null);
        HDFSDDF csvDDF = manager.newDDF("", null);
        assert (cleanFolderDDF.getIsDir() == true);
        assert(jsonDDF.getIsDir() == false);
        assert(csvDDF.getIsDir() == false);
        assert(jsonDDF.getDataFormat().equals(DataFormat.JSON));
        assert(csvDDF.getDataFormat().equals(DataFormat.CSV));
        try {
            // Test on non-exist folder/file. Should throw exception.
            HDFSDDF nonExistDDF = manager.newDDF("", null);
            assert (false);
        } catch (Exception e) {

        }
        try {
            HDFSDDF nonExistDDF2 = manager.newDDF("", null);
            assert (false);
        } catch (Exception e) {

        }
        // TODO: Add pqt
    }


    @Test
    public void testHead() throws DDFException {
        HDFSDDF csvDDF = manager.newDDF("", null);
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

        HDFSDDF folderDDF = manager.newDDF("", null);
        rows = manager.head(folderDDF, 5);
        assert (rows.size() == 4);

        HDFSDDF ddf1024 = manager.newDDF("", null);
        rows = manager.head(ddf1024, 9999);
        assert (rows.size() == 1000);
    }

}
