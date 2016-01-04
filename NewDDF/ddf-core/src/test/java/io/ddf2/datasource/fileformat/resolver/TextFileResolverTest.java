package io.ddf2.datasource.fileformat.resolver;

import io.ddf2.datasource.schema.ISchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 1/4/16.
 */
public class TextFileResolverTest {


    TextFileResolver textFileResolver = new TextFileResolver();

    @org.junit.Test
    public void testResolve() throws Exception {
        List<List<String>> sampleRows = new ArrayList<>();
        sampleRows.add(Arrays.asList("user1","10","male","1.0","true"));
        sampleRows.add(Arrays.asList("user1","10.5","male","1.0","true"));
        sampleRows.add(Arrays.asList("user1","10","male","1.0","true","extend"));
        sampleRows.add(Arrays.asList("user1","10","male","1.0","true"));
        ISchema schema = textFileResolver.resolve(Arrays.asList("c1", "c2", "c3"), sampleRows);
        System.out.println("Resolve Schema : " + schema);
    }
}