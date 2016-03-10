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
        manager = new HDFSDDFManager("hdfs://localhost:9000");
    }

    @Test
    public void testListing() throws DDFException {
        List<String> files = manager.listFiles("/usr");
        LOG.info("========== files ==========");
        for (String file: files) {
            LOG.info(file);
        }

    }

    @Test
    public void testCreateDDF() throws DDFException {
        try {
            HDFSDDF folderDDF = manager.newDDF("/user/testFolder", null);
            assert(folderDDF.getIsDir() == true);
        } catch (Exception e) {

        }

        HDFSDDF cleanFolderDDF = manager.newDDF("/user/estFolder", null);
        HDFSDDF jsonDDF = manager.newDDF("/user/a.json", null);
        HDFSDDF csvDDF = manager.newDDF("/user/hasHeader.csv", null);
        assert (cleanFolderDDF.getIsDir() == true);
        assert(jsonDDF.getIsDir() == false);
        assert(csvDDF.getIsDir() == false);
        assert(jsonDDF.getDataFormat().equals(DataFormat.JSON));
        assert(csvDDF.getDataFormat().equals(DataFormat.CSV));
        try {
            // Test on non-exist folder/file. Should throw exception.
            HDFSDDF nonExistDDF = manager.newDDF("/user/jing/nonexist.csv", null);
            assert (false);
        } catch (Exception e) {

        }
        try {
            HDFSDDF nonExistDDF2 = manager.newDDF("/user/jing/nonexsist", null);
            assert (false);
        } catch (Exception e) {

        }

        HDFSDDF pqtDDF = manager.newDDF("/usr/parquet/", null);
        assert (pqtDDF.getIsDir() == true);
        assert (pqtDDF.getDataFormat().equals(DataFormat.PARQUET));


        HDFSDDF avroDDF = manager.newDDF("/usr/avro/", null);
        assert (avroDDF.getIsDir() == true);
        assert (avroDDF.getDataFormat().equals(DataFormat.AVRO));

        HDFSDDF orcDDF = manager.newDDF("/usr/orc/", null);
        assert (orcDDF.getIsDir() == true);
        assert (orcDDF.getDataFormat().equals(DataFormat.ORC));
    }


    @Test
    public void testHead() throws DDFException {
        HDFSDDF csvDDF = manager.newDDF("/user/jing/year.csv", null);
        List<String> rows = manager.head(csvDDF, 5);
        assert(rows.size() == 2);
        LOG.info("========== content of year.csv ==========");
        for (String s : rows) {
            System.out.println(s);
        }

        rows = manager.head(csvDDF, 20000);
        assert(rows.size() == 2);

        rows = manager.head(csvDDF, 1000);
        assert(rows.size() == 2);

        HDFSDDF folderDDF = manager.newDDF("/user/jing/testFolder", null);
        rows = manager.head(folderDDF, 5);
        assert (rows.size() == 4);

        HDFSDDF ddf1024 = manager.newDDF("/user/jing/1024.csv", null);
        rows = manager.head(ddf1024, 9999);
        assert (rows.size() == 1000);
    }

}
