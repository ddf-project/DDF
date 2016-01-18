package io.ddf.rest;

import com.sun.deploy.net.HttpResponse;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jing on 1/18/16.
 */
public class RestDDFManagerTests {
    public static RestDDFManager restDDFManager;
    public static Logger LOG;

    @Test
    public void testDDFConfig() {
        Assert.assertEquals("rest", restDDFManager.getEngine());
    }

    @BeforeClass
    public static void initManager() throws Exception {
        LOG = LoggerFactory.getLogger(RestDDFManagerTests.class);
        restDDFManager = new RestDDFManager();
    }

    @Test
    public void testCreateDDF() throws DDFException {
        //http://api.openweathermap.org/data/2.5/weather?q=London,uk&appid=2de143494c0b295cca9337e1e96b00e0
        RestDDF ddf = restDDFManager.newDDF("api.openweathermap.org/data/2.5/weather?lat=35&lon=139");
    }

    @Test
    public void testGetData() throws DDFException {
        RestDDF ddf = restDDFManager.newDDF("api.openweathermap.org/data/2.5/weather?lat=35&lon=139");

    }

}
