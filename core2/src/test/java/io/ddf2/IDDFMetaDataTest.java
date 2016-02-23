package io.ddf2;

import io.ddf2.datasource.filesystem.fileformat.CSVFile;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import utils.TestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created by sangdn on 1/6/16.
 */
public class IDDFMetaDataTest {


    /**
     * Do overral IDDFMetaData Test
     *
     * @param ddfManager
     */
    public static void DDFMetaDataOverralTest(IDDFManager ddfManager) throws DDFException {
        IDDFMetaData ddfMetaData = ddfManager.getDDFMetaData();

        //create some ddf for test
        String rootPath = "/tmp/";
        String[] ddfName4Test = new String[]{"ddf_1", "ddf_2", "ddf_3", "ddf_4", "ddf_5"};

        for(int i = 0; i < ddfName4Test.length; ++i){
            ddfMetaData.dropDDF(ddfName4Test[i]);
        }

        for (String ddfName : ddfName4Test) {
            String fileName = rootPath + ddfName;
            TestUtils.makeCSVFileUserInfo(fileName, 10, TestUtils.COMMA_SEPARATOR);
            LocalFileDataSource LocalFileDataSource = io.ddf2.datasource.filesystem.LocalFileDataSource.builder()
                    .addPath(fileName)
                    .setFileFormat(new CSVFile(CSVFile.COMMA_SEPARATOR))
                    .build();
            ddfManager.newDDF(ddfName, LocalFileDataSource);
        }
        //TEST getAllDDFName
        assert ddfMetaData.getNumDDF() == ddfName4Test.length;
        Set<String> allDDFNames = ddfMetaData.getAllDDFNames();
        assert Arrays.equals(allDDFNames.toArray(new String[0]),ddfName4Test);

        ddfMetaData.dropDDF(ddfName4Test[0]);
        assert ddfMetaData.getAllDDFNames().contains(ddfName4Test[0]) == false;
        for(int i = 1; i < ddfName4Test.length; ++i){
            ddfMetaData.dropDDF(ddfName4Test[i]);
        }



    }
}