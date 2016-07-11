package io.ddf.hdfs;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void testValidation() {
        try {
            new HDFSDDFManager("invalidpath");
            assert false;
        } catch (DDFException e) {}
    }

    @Test
    public void testListing() throws DDFException {
        List<String> files = manager.listFiles("/test_pe");
        assert (files.size() > 0);
    }

    private Map<String, String> withFormat(String format) {
        Map<String, String> options = new HashMap<>();
        options.put("format", format);
        return options;
    }
}
