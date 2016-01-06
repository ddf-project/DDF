package io.ddf2.spark;

import io.ddf2.*;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 1/6/16.
 */
public class SparkDDFMetadataTest {

    @Test
    public void overralTest() throws DDFException {
        IDDFManager ddfManager = DDFManager.getInstance(SparkDDFManager.class, Collections.emptyMap());
        IDDFMetaDataTest.DDFMetaDataOverralTest(ddfManager);
    }
}