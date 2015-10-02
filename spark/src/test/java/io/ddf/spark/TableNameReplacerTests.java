package io.ddf.spark;

import io.ddf.DDF;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;


/**
 * Created by jing on 6/30/15.
 */
public class TableNameReplacerTests {


    public static DDFManager manager;
    public static CCJSqlParserManager parser;

    @Test
    public void testUnion() {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        String sqlcmd = "select * from ddf://adatao/a union select * from " +
                "ddf://adatao/b";
        try {
            Statement statement = parser.parse(new StringReader(sqlcmd));
            // System.out.println(statement.toString());
            int a = 2;
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAlias() {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        String sqlcmd = "select T0.id from (select tmp.id from ddf://adatao/a" +
                " " +
                "tmp) T0";
        try {
            Statement statement = parser.parse(new StringReader(sqlcmd));
            // statement = tableNameReplacer.run(statement);
            // System.out.println(statement.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testRealQuery() {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        String sqlcmd = "SELECT from_unixtime(round(timestamp) - 7*3600, 'HH') hour,\n" +
                "        count(1) count,\n" +
                "        avg(observationnum_temp) avgTemp, \n" +
                "        avg(observationnum_feels_like) avgFeelLike,\n" +
                "        avg(observationnum_temp_min_24hour) avgMinTemp, \n" +
                "        avg(observationnum_temp_max_24hour) avgMaxTemp\n" +
                "FROM cod_sample2 \n" +
                "WHERE latitude > 30 and latitude < 40 and longtitude > -125 and longtitude < -115\n" +
                "GROUP BY from_unixtime(round(timestamp) - 7*3600, 'HH') \n" +
                "HAVING hour is not null\n" +
                "ORDER BY hour";
        try {
            Statement statement = parser.parse(new StringReader(sqlcmd));
            // System.out.println(statement.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        sqlcmd = "SELECT unix_timestamp(from_unixtime(round(timestamp) - 7*3600, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') time,\n" +
                "        avg(observationnum_temp) avgTemp, \n" +
                "        avg(observationnum_temp_min_24hour) avg24HMinTemp, \n" +
                "        avg(observationnum_temp_max_24hour) avg24HMaxTemp\n" +
                "FROM cod_sample2 \n" +
                "WHERE latitude > 30 and latitude < 40 and longtitude > -125 and longtitude < -115\n" +
                "GROUP BY unix_timestamp(from_unixtime(round(timestamp) - 7*3600, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH')\n" +
                "HAVING time is not null\n" +
                "ORDER BY time";
        try {
            Statement statement = parser.parse(new StringReader(sqlcmd));
            // System.out.println(statement.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();;
            assert false;
        }

        sqlcmd = "select *\n" +
                "from (select country_cd, count(1) c from location_thunderbird group by country_cd) tmp\n" +
                "order by c desc\n" +
                "limit 10";

        try {
            Statement statement = parser.parse(new StringReader(sqlcmd));
            // System.out.println(statement.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert false;
        }

    }


    /**
     * @brief Test complex query with full uri.
     * @throws DDFException
     */
    @Test
    public void testComplexQuery() throws  DDFException {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        String sqlcmd =
                "With p as " +
                "(Select * from ddf://adatao/a) " +
                "select sum(ddf://adatao/a.depdelay) " +
                "from ddf://adatao/a  TABLESAMPLE(100 percent), ddf://adatao/b " +
                "on ddf://adatao/a.id = ddf://adatao/b.id " +
                "where ddf://adatao/a.id > 1 AND ddf://adatao/a.id < 3 or ddf://adatao/a.year > 2000 " +
                "group by ddf://adatao/a.year " +
                "having sum(ddf://adatao/a.depdelay) < 100 " +
                "order by ddf://adatao/a.year";
        Statement statement = null;
        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }
        try {
            // statement = tableNameReplacer.run(statement);
        } catch (Exception e) {
            e.printStackTrace();

            assert (false);
            //assert(false);
        }
    }


    /**
     * @brief Test full uri replacement.
     * @throws DDFException
     */
    @Test
    public  void testFullURI() throws  DDFException {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        String sqlcmd = "select SUM(ddf://adatao/a.b) from ddf://adatao/a group by ddf://adatao/a.a";
        Statement statement = null;
        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }

        try {
            // statement = tableNameReplacer.run(statement);
        } catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
        assert(statement.toString().equals("SELECT SUM(tablename1.b) FROM tablename1 GROUP BY tablename1.a"));
    }

    public Statement testFullURISingle(String sqlcmd) throws Exception {
        Statement statement = parser.parse(new StringReader(sqlcmd));
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        return null;
        // return tableNameReplacer.run(statement);
    }

    @Test
    public void batchTestFullURI() throws DDFException {
        String[] selectItems = {
               " * ",
               " ddf://adatao/a.year ",
               " round(ddf://adatao/a.year) ",
               " year "
        };

        String[] joinTypes = {
                " , ",
                " join ",
                " LEFT OUTER JOIN ",
                " LEFT JOIN ",
                " RIGHT OUTER JOIN ",
                " FULL OUTER JOIN ",
                " CROSS JOIN "
        };

        String[] joinConds = {
                " ",
                " ON ddf://adatao/a.id = ddf://adatao/b.id "
        };

        String[] wehereCaluses = {
          " WHERE year > id ",
          " WHERE ddf://adatao/a.id > 1 AND ddf://adatao/b.id < 3 "
        };

        String[] sortOptions = {
            " ",
            " Cluster by ddf://adatao/a.year ",
            " Distribute By ddf://adatao/a.year Sort by ddf://adatao/a.id "
        };

        for (String selectItem : selectItems) {
            for (String joinType : joinTypes) {
                for (String joinCond : joinConds) {
                    for (String whereClause : wehereCaluses) {
                        for (String sortOption : sortOptions) {
                            String sqlcmd = "Select" + selectItem + "from ddf://adatao/a" +
                                    joinType + "ddf://adatao/b" + joinCond + whereClause + sortOption;
                            // System.out.println(sqlcmd);
                            try {
                                Statement statement = testFullURISingle(sqlcmd);
                                // System.out.println(statement.toString());
                            } catch (Exception e) {
                                e.printStackTrace();
                                assert false;
                            }
                        }
                    }
                }
            }
        }


    }

    /**
     * @brief Test sql with namespace specified.
     * @throws DDFException
     */
    @Test
    public void testNamespace() throws  DDFException {
        TableNameReplacer tableNameReplacer  = new TableNameReplacer(manager);
        // TableNameReplacer tableNameReplacer  = new TableNameReplacer
        //        (manager, "adatao");

        String sqlcmd = "select a.b from a";
        Statement statement = null;
        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }
        try {
            // tableNameReplacer.run(statement);
        } catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }

        assert(statement.toString().equals("SELECT tablename1.b FROM tablename1"));

    }

    /**
     * @brief Test sql with list.
     * @throws DDFException
     */
    @Test
    public void testList() throws  DDFException {
        String[] uris={"ddf://adatao/a", "ddf://adatao/b"};
        // TableNameReplacer tableNameReplacer = new TableNameReplacer(manager,
        //        Arrays.asList(uris));
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager,
                    null);

        String sqlcmd = "select {1}.a,{2}.b from {1}";
        Statement statement = null;

        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }

        try {
            // tableNameReplacer.run(statement);
        } catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
        assert(statement.toString().equals("SELECT tablename1.a, tablename2.b FROM tablename1"));
    }

    /**
     * @brief Test udfs.
     * @throws DDFException
     */
    @Test
    public void testUDF() throws  DDFException {
        String[] singleParamUDFs = {
                "round", "floor", "ceil", "ceiling", "rand", "exp", "ln", "log10", "log2"
        };

        String[] doubleParamUDFs = {
                "log", "pow"
        };


        TableNameReplacer tableNameReplacer = new TableNameReplacer(this.manager);
        String sqlcmd = "select %s(ddf://adatao/a.year) from ddf://adatao/a";
        String doubleSqlCmd = "select %s(ddf://adatao/a.year, ddf://adatao/a.rev) from ddf://adatao/a";
        for (String udfname : singleParamUDFs) {
            String newSqlCmd = String.format(sqlcmd, udfname);
            try {
                Statement statement = parser.parse(new StringReader(newSqlCmd));
                // statement = tableNameReplacer.run(statement);
                assert (statement.toString().toLowerCase().equals(
                        String.format("select %s(tablename1.year) from tablename1", udfname)
                ));
            } catch (JSQLParserException e) {
                e.printStackTrace();
                assert(false);
            } catch (Exception e) {
                e.printStackTrace();
                assert false;
            }
        }

        for (String udfname : doubleParamUDFs) {
            String newSqlCmd = String.format(doubleSqlCmd, udfname);
            try {
                Statement statement = parser.parse(new StringReader(newSqlCmd));
                // statement = tableNameReplacer.run(statement);
                assert (statement.toString().toLowerCase().equals(
                        String.format("select %s(tablename1.year, tablename1.rev) from tablename1",
                                udfname)));
            } catch (Exception e) {
                e.printStackTrace();
                assert false;
            }
        }

    }

    /**
     * @brief Test ordinary spark query.
     * @throws DDFException
     */
    @Test
    public  void testLoading() throws  DDFException {
        SQLDataSourceDescriptor sqlDataSourceDescriptor = new SQLDataSourceDescriptor(null, "SparkSQL", null, null, null);
        manager.sql("drop table if exists airline", sqlDataSourceDescriptor);

        manager.sql("create table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
               + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", sqlDataSourceDescriptor);

        manager.sql("load data local inpath '../resources/test/airline.csv' " +
                        "into table airline",
                sqlDataSourceDescriptor);

        DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
                + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline",
                sqlDataSourceDescriptor);
        this.manager.setDDFName(ddf, "airlineDDF");
        // DDF sql2ddfRet = manager.sql2ddf("select * from " +
        //        "ddf://adatao/airlineDDF");
    }

    @Test
    public void BatchTestArithOps() {
        String[] arithOps = {"+", "-", "*", "/", "%", "&", "|", "^"};
        String sqlcmd = "select ddf://adatao/a.id %s ddf://adatao/a.id2 from ddf://adatao/a";
        TableNameReplacer tableNameReplacer = new TableNameReplacer(this.manager);
        for (String arithOp : arithOps) {
            String newSqlCmd = String.format(sqlcmd, arithOp);
            Statement statement = null;
            try {
                statement = this.testFullURISingle(newSqlCmd);
                // System.out.println(statement);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void BatchTestRelationalOps() {
        String[] relationalOps = {"=", "<=>", "<>", "!=", "<", "<=", ">", ">="};
        String sqlcmd = "select * from ddf://adatao/a where ddf://adatao/a.id %s ddf://adatao/a.id2";
        TableNameReplacer tableNameReplacer = new TableNameReplacer(this.manager);
        for (String relationalOp : relationalOps) {
            String newSqlCmd = String.format(sqlcmd, relationalOp);
            try {
                Statement statement = this.testFullURISingle(newSqlCmd);
                // System.out.println(statement);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String[] relationalLiterals = {
                " ddf://adatao/a.id between ddf://adatao/a.id1 and ddf://adatao/a.id2 ",
                " ddf://adatao/a.id not between ddf://adatao/a.id1 and ddf://adatao/a.id2 ",
                " ddf://adatao/a.id is null ",
                " ddf://adatao/a.id is not null ",
                " ddf://adatao/a.id LIKE ddf://adatao/a.id2 ",
                " ddf://adatao/a.id RLIKE ddf://adatao/a.id2 ",
                " ddf://adatao/a.id REGEXP ddf://adatao/a.id2 ",
        };

        for (String literal : relationalLiterals) {
            String newSqlCmd = "Select * from ddf://adatao/a where " + literal;
            try {
                Statement statement = this.testFullURISingle(newSqlCmd);
                // System.out.println(statement);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String testSqlCmd = "select * from ddf://adatao/a where ddf://adatao/a.id regexp ddf://adatao/a.id2";
        try {

            Statement newStat = parser.parse(new StringReader(testSqlCmd));
            // newStat = tableNameReplacer.run(newStat);

        } catch (Exception e) {
            e.printStackTrace();
        }
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
        manager = DDFManager.get(DDFManager.EngineType.SPARK);
        Schema schema = new Schema("tablename1", "d  d,d  d");
        DDF ddf = manager.newDDF(manager, new Class<?>[] { DDFManager.class
                }, "adatao", "a",
                schema);
        Schema schema2 = new Schema("tablename2", "d  d,d  d");
        DDF ddf2 = manager.newDDF(manager, new Class<?>[] { DDFManager.class
                }, "adatao", "b",
                schema2);

        parser = new CCJSqlParserManager();
    }

    @AfterClass
    public static void stopServer() throws Exception {

        manager.shutdown();
    }

}
