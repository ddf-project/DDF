import io.ddf.DDF;
import io.ddf.datasource.JDBCDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;

/**
 * Created by nhanitvn on 11/8/15.
 */
public class DataLoadingTests extends BaseTest {
    @Test
    public void testLoadFromJDBC() throws DDFException, URISyntaxException {
        // load data from a MySQL
        JDBCDataSourceDescriptor desc = new JDBCDataSourceDescriptor("jdbc:mysql://localhost:3306/test", "pauser", "papwd", "mtcars");

        DDF ddf = manager.load(desc);
        Assert.assertTrue(ddf != null);
        Assert.assertEquals(ddf.getNumColumns(), 11);
        Assert.assertEquals(ddf.getNumRows(), 32);
    }
}
