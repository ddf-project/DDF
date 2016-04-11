package io.ddf.hdfs;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        LOG = LoggerFactory.getLogger(HDFSDDFManagerTests.class);
        manager = new HDFSDDFManager(System.getenv("HDFS_URI"));
    }

    @Test
    public void testListing() throws DDFException {
        List<String> files = manager.listFiles("/test_pe");
        assert (files.size() > 0);

    }

    @Test
    public void testCreateDDF() throws DDFException {

        HDFSDDF cleanFolderDDF = manager.newDDF("/test_pe/csv/multiple", null, null);
        HDFSDDF jsonDDF = manager.newDDF("/test_pe/json/noheader", null, null);
        HDFSDDF csvDDF = manager.newDDF("/test_pe/csv/noheader", null, null);
        assert (cleanFolderDDF.getIsDir() == true);
        assert(jsonDDF.getIsDir() == true);
        assert(csvDDF.getIsDir() == true);
        assert(jsonDDF.getDataFormat().equals(DataFormat.JSON));
        assert(csvDDF.getDataFormat().equals(DataFormat.CSV));
        try {
            // Test on non-exist folder/file. Should throw exception.
            HDFSDDF nonExistDDF = manager.newDDF("/test_pe/nonexist.csv", null, null);
            assert (false);
        } catch (Exception e) {

        }
        try {
            HDFSDDF nonExistDDF2 = manager.newDDF("/test_pe/nonexist", null, null);
            assert (false);
        } catch (Exception e) {

        }

        HDFSDDF pqtDDF = manager.newDDF("/test_pe/parquet/default", null, null);
        assert (pqtDDF.getIsDir() == true);
        assert (pqtDDF.getDataFormat().equals(DataFormat.PQT));


        HDFSDDF avroDDF = manager.newDDF("/test_pe/avro/single", null, null);
        assert (avroDDF.getIsDir() == true);
        assert (avroDDF.getDataFormat().equals(DataFormat.AVRO));

        HDFSDDF orcDDF = manager.newDDF("/test_pe/orc/default", null, null);
        assert (orcDDF.getIsDir() == true);
        assert (orcDDF.getDataFormat().equals(DataFormat.ORC));

        HDFSDDF noExtensionDDF = manager.newDDF("/test_pe/extra/schema/lines-with-different-cols/lines-with-different-cols", null, null);
        assert (noExtensionDDF.getIsDir() == false);
        assert (noExtensionDDF.getDataFormat().equals(DataFormat.CSV));

        HDFSDDF emptyDDF = manager.newDDF("/test_pe/extra/empty-folder/", null, null);
        assert (emptyDDF.getIsDir() == true);
        assert (emptyDDF.getDataFormat().equals(DataFormat.CSV));

        try {
            HDFSDDF folderDDF = manager.newDDF("/test_pe/", null, null);
            assert false;
        } catch (Exception e) {
        }

        try {
            HDFSDDF mixedDDF = manager.newDDF("/test_pe/extra/format/mixed-csv-tsv/", null, null);
            assert (false);
        } catch (Exception e) {
            assert (e.getMessage().contains("more than 1"));
        }
    }


    @Test
    public void testHead() throws DDFException {
        HDFSDDF csvDDF = manager.newDDF("/test_pe/csv/small/1.csv", null, null);
        List<String> rows = manager.head(csvDDF, 5);
        assert(rows.size() == 2);

        rows = manager.head(csvDDF, 20000);
        assert(rows.size() == 2);

        rows = manager.head(csvDDF, 1000);
        assert(rows.size() == 2);

        HDFSDDF folderDDF = manager.newDDF("/test_pe/csv/small", null, null);
        rows = manager.head(folderDDF, 5);
        assert (rows.size() == 4);

        HDFSDDF ddf1024 = manager.newDDF("/test_pe/csv/fixed_len/1024.csv", null, null);
        rows = manager.head(ddf1024, 9999);
        assert (rows.size() == 1000);
    }

}
