package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.schema.ISchema;
import utils.TestUtils;

/**
 * Created by sangdn on 1/8/16.
 * BaseCommonTest will help to test all concrete engine.
 * An concrete test should call this every time they change their engine
 */
public class BaseCommonTest {

    /**
     * Execute sql test on concrete manager.
     * @param manager : Concrete Manager
     * @param dataSource : Concrete datasource to test, required contains data
     * @throws DDFException
     */
    public static void TestSql(IDDFManager manager,IDataSource dataSource) throws DDFException {

        System.out.println("BaseCommonTest::TestSql Begin Test");
        String ddfName = TestUtils.genString();
        manager.getDDFMetaData().dropDDF(ddfName);
        IDDF ddf = manager.newDDF(ddfName,dataSource);
        ISchema schema = ddf.getSchema();
        assert schema != null;
        assert  schema.getNumColumn() > 0;
        assert schema.getNumColumn() > 0;
        assert schema.getColumns().size() > 0;
        //execute simple sql
        ISqlResult sqlResult = ddf.sql("select * from " + ddf.getDDFName());
//        while(sqlResult.next()){
//            System.out.println(sqlResult.getRaw());
//            assert sqlResult.getSchema().equals(schema);
//        }
        System.out.println("Schema" + schema.toString());
        manager.getDDFMetaData().dropDDF(ddfName);
        System.out.println("BaseCommonTest::TestSql End Test");



    }
}
