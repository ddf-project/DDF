package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.schema.ISchema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import utils.TestUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

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
    public static void TestSql(IDDFManager manager,IDataSource dataSource) throws DDFException, SQLException, IOException {

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
        while(sqlResult.next()){
            System.out.println(sqlResult.getRaw());

        }
        System.out.println("Schema" + schema.toString());
        manager.getDDFMetaData().dropDDF(ddfName);
        System.out.println("BaseCommonTest::TestSql End Test");



    }

    /**
     * Execute testsuite on IDDFMetaData @see IDDFMetaData
     * @param ddfMetaData concrete DDFMetaData to test
     */
    public static void testDDFMetaData(IDDFMetaData ddfMetaData) throws DDFException {
        Set<String> allDDFNames = ddfMetaData.getAllDDFNames();
        Set<Pair<String, ISchema>> allDDFNameWithSchema = ddfMetaData.getAllDDFNameWithSchema();
        assert  allDDFNames.size() == allDDFNameWithSchema.size();


        Iterator<String> itDDFName = allDDFNames.iterator();
        while(itDDFName.hasNext()){
            String ddfName = itDDFName.next();
            ISchema ddfSchema = ddfMetaData.getDDFSchema(ddfName);
//            assert  allDDFNameWithSchema.contains(new ImmutablePair<String,ISchema>(ddfName,ddfSchema));
            System.out.println("--------------------------");
            System.out.println(ddfName);
            System.out.println(ddfSchema);
        }


    }
}
