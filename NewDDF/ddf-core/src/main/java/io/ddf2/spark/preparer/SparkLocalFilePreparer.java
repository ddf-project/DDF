package io.ddf2.spark.preparer;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.fileformat.TextFileFormat;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.spark.SparkUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.util.*;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * SparkLocalFilePreparer will prepare for SparkDDF from Local File.
 * + Ensure Schema
 * + Create HiveTable
 * + Load Local Data Into HiveTable.
 */

public class SparkLocalFilePreparer implements IDataSourcePreparer {
    protected HiveContext hiveContext;
    protected SparkContext sparkContext;
    protected static final int NUM_SAMPLE_ROW = 10; //Num Sample Row for inferschema.
    protected static BasicTextFileResolver textFileResolver = new BasicTextFileResolver();

    public SparkLocalFilePreparer(SparkContext sparkContext, HiveContext hiveContext) {

        this.sparkContext = sparkContext;
        this.hiveContext = hiveContext;
    }

    @Override
    public IDataSource prepare(String ddfName, IDataSource dataSource) throws PrepareDataSourceException {
        try {
            LocalFileDataSource fileDataSource = (LocalFileDataSource) dataSource;
            TextFileFormat textFileFormat = (TextFileFormat) fileDataSource.getFileFormat();
            ISchema schema = dataSource.getSchema();

            //check if schema exist, if not, inferschema.
            if (schema == null) {
                schema = inferSchema(fileDataSource);
            }

            prepareData(ddfName, schema.getColumns(), textFileFormat, fileDataSource.getPaths());
            //build prepared already datasource

            return LocalFileDataSource.builder()
                    .addPaths(fileDataSource.getPaths())
                    .setFileFormat(fileDataSource.getFileFormat())
                    .setSchema(schema)
                    .setCreatedTime(System.currentTimeMillis())
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            throw new PrepareDataSourceException(e.getMessage());
        }

    }
    protected ISchema inferSchema(LocalFileDataSource localFileDataSource) throws Exception {
        TextFileFormat textFileFormat = (TextFileFormat) localFileDataSource.getFileFormat();
        String fileName = localFileDataSource.getPaths().get(0);
        List<String> preferColumnName = new ArrayList<>();
        List<List<String>> sampleRows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
            if (textFileFormat.firstRowIsHeader()) {
                String header = br.readLine();
                String[] split = header.split(textFileFormat.getDelimiter());
                preferColumnName = Arrays.asList(split);
            } else {
                for (int i = 0; i < NUM_SAMPLE_ROW; ++i) {
                    String row = br.readLine();
                    if (row == null) break;
                    String[] columns = row.split(textFileFormat.getDelimiter());
                    sampleRows.add(Arrays.asList(columns));
                }
            }
        }

        ISchema schema = textFileResolver.resolve(preferColumnName, sampleRows);
        assert schema != null && schema.getColumns() != null && schema.getNumColumn() > 0;
        return schema;

    }


    protected void prepareData(String ddfName, List<IColumn> columns, TextFileFormat textFileFormat, List<String> paths) throws PrepareDataSourceException {

        //Create Table name with Schema
        StringBuilder strCreateTable = new StringBuilder();
        strCreateTable.append("CREATE TABLE ").append(ddfName).append(" ( ");
        StringBuilder sbTableSchema = new StringBuilder();
        for (int i = 0; i < columns.size(); ++i) {
            if (sbTableSchema.length() > 0) sbTableSchema.append(",");
            String name = columns.get(i).getName();
            Class javaType = columns.get(i).getType();
            String hiveTypeName = SparkUtils.javaTypeToHiveName(javaType);

            sbTableSchema.append(name).append(" ").append(hiveTypeName);
        }
        strCreateTable.append(sbTableSchema.toString()).append(" )");
        strCreateTable.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + textFileFormat.getDelimiter() + "'");

        if (textFileFormat.firstRowIsHeader())
            strCreateTable.append(" tblproperties('skip.header.line.count'='1')");
        hiveContext.sql(strCreateTable.toString());

        //load local data from paths
        for (String path : paths) {
            String strLoadData = "load data local inpath '" + path + "' into table " + ddfName;
            hiveContext.sql(strLoadData);
        }

    }

}
