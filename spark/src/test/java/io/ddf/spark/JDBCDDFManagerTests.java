package io.ddf.spark;

import io.ddf.DDF;
import io.ddf.DDFCoordinator;
import io.ddf.DDFManager;
import io.ddf.TableNameReplacer;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.datasource.*;
import io.ddf.exception.DDFException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;


/**
 * Created by jing on 6/30/15.
 */
public class JDBCDDFManagerTests {


    @Test
    public void testJDBC() throws Exception {
        DDFCoordinator coordinator = new DDFCoordinator();
        JDBCDataSourceDescriptor mysqlDS
                = new JDBCDataSourceDescriptor("jdbc:mysql://localhost/test",
                        "pauser", "papwd", null);
        DDFManager mysqlManager = coordinator.initEngine("mysql", "jdbc",
                mysqlDS);

        SqlResult sqlRet = mysqlManager.sql("show tables", "jdbc");
        SqlResult selectRet = mysqlManager.sql("select * from a", "jdbc");

        DDFManager sparkManager = DDFManager.get("spark");
        sparkManager.setEngineName("spark");
        sparkManager.setEngineType("spark");
        sparkManager.setDDFCoordinator(coordinator);
        coordinator.getDDFManagerList().add(sparkManager);
        coordinator.getName2DDFManager().put("spark", sparkManager);

        DDF ddf = sparkManager.transferByTable("mysql", "a");

        sparkManager.setEngineName("spark");
        Schema schema = new Schema("tablename1", "d  d,d  d");

    }

    // static Logger LOG;

    @BeforeClass
    public static void startServer() throws Exception {
        Thread.sleep(1000);

        // LOG = LoggerFactory.getLogger(BaseTest.class);
        // manager = DDFManager.get("spark");
        /*
        manager = DDFManager.get("jdbc", new JDBCDataSourceDescriptor(new
                DataSourceURI("jdbc:mysql://localhost/testdb"), new
                JDBCDataSourceDescriptor.JDBCDataSourceCredentials("pauser",
                "papwd"), null));
        DataSourceDescriptor ds = manager.getDataSourceDescriptor();
        if (ds instanceof
                JDBCDataSourceDescriptor) {
            System.out.println("hello");
        }
        DDF ret = manager.sql2ddf("select * from testtable", "jdbc");*/
        // Add 2 test ddfs.

    }

    @AfterClass
    public static void stopServer() throws Exception {

    }

}