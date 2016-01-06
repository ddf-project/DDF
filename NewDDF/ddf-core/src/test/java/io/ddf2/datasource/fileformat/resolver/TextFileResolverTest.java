package io.ddf2.datasource.fileformat.resolver;

import io.ddf2.datasource.schema.IColumn;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.spark.preparer.BasicTextFileResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 1/4/16.
 */
public class TextFileResolverTest {


    BasicTextFileResolver textFileResolver = new BasicTextFileResolver();

    @org.junit.Test
    public void testResolve() throws Exception {
        List<List<String>> sampleRows = new ArrayList<>();
        sampleRows.add(Arrays.asList("user1","10","male","1.0","true"));
        sampleRows.add(Arrays.asList("user1","10.5","male","1.0","true"));
        sampleRows.add(Arrays.asList("user1","10","male","1.0","true","extend"));
        sampleRows.add(Arrays.asList("user1","10","male","1.0","true"));
        ISchema schema = textFileResolver.resolve(Arrays.asList("c0", "c1", "c2"), sampleRows);
        assert schema.getNumColumn() == 6;
        List<IColumn> columns = schema.getColumns();
        assert columns != null;
        assert  columns.get(0).getName().equals("c0");
        assert  columns.get(0).getType().equals(String.class);

        assert  columns.get(1).getName().equals("c1");
        assert  columns.get(1).getType().equals(Long.class);

        assert  columns.get(2).getName().equals("c2");
        assert  columns.get(2).getType().equals(String.class);

        assert  columns.get(3).getName().equals("col_3");
        assert  columns.get(3).getType().equals(Double.class);

        assert  columns.get(4).getName().equals("col_4");
        assert  columns.get(4).getType().equals(Boolean.class);

        assert  columns.get(5).getName().equals("col_5");
        assert  columns.get(5).getType().equals(String.class);

    }
}