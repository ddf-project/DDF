package io.ddf.spark.datasource;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.datasource.*;
import io.ddf.exception.DDFException;
import io.ddf.spark.SparkDDFManager;
import io.ddf.spark.util.SparkUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.net.URI;
import java.util.*;

/**
 * Created by jing on 10/29/15.
 */
public class SparkDataSourceManager extends DataSourceManager {

    public SparkDataSourceManager(DDFManager manager) {
        super(manager);
    }

    @Override
    public DDF loadSpecialFormat(DataFormat format, URI fileURI, Boolean flatten) throws DDFException {
        SparkDDFManager sparkDDFManager = (SparkDDFManager)mDDFManager;
        HiveContext sqlContext = sparkDDFManager.getHiveContext();
        DataFrame jdf = null;
        switch (format) {
            case JSON:
                jdf = sqlContext.jsonFile(fileURI.toString());
                break;
            case PQT:
                jdf = sqlContext.parquetFile(fileURI.toString());
                break;
            default:
                throw new DDFException(String.format("Unsupported data format: %s", format.toString()));
        }

        DataFrame df = SparkUtils.getDataFrameWithValidColnames(jdf);
        DDF ddf = sparkDDFManager.newDDF(sparkDDFManager, df, new Class<?>[]{DataFrame.class},
            null, SparkUtils.schemaFromDataFrame(df));

        if(flatten == true)
            return ddf.getFlattenedDDF();
        else
            return ddf;
    }

    @Override
    public DDF loadFromJDBC(JDBCDataSourceDescriptor dataSource) throws DDFException {
        SparkDDFManager sparkDDFManager = (SparkDDFManager)mDDFManager;
        HiveContext sqlContext = sparkDDFManager.getHiveContext();

        JDBCDataSourceCredentials cred = (JDBCDataSourceCredentials)dataSource.getDataSourceCredentials();
        String fullURL = dataSource.getDataSourceUri().getUri().toString();
        if (cred.getUsername() != null &&  !cred.getUsername().equals("")) {
            fullURL += String.format("?user=%s&password=%s", cred.getUsername(), cred.getPassword());
        }

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", fullURL);
        options.put("dbtable", dataSource.getDbTable());
        DataFrame df = sqlContext.load("jdbc", options);

        DDF ddf = sparkDDFManager.newDDF(sparkDDFManager, df, new Class<?>[]{DataFrame.class},
            null, SparkUtils.schemaFromDataFrame(df));
        // TODO?
        ddf.getRepresentationHandler().get(RDD.class, Row.class);
        ddf.getMetaDataHandler().setDataSourceDescriptor(dataSource);
        return ddf;
    }

    @Override
    public DDF loadTextFile(DataSourceDescriptor dataSource) throws DDFException {
         String hiveTableName = UUID.randomUUID().toString().replace("-", "_");
         StringBuilder stringBuilder = new StringBuilder();
        List<String> columnNames = new ArrayList<>();
         List<Schema.Column> columnList = dataSource.getDataSourceSchema().getColumns();
         for (int i = 0; i < columnList.size(); ++i) {
             columnNames.add(columnList.get(i).getName());
             if (i == 0) {
             stringBuilder.append(columnList.get(i).getName() + " " + columnList.get(i).getType());
             } else {
             stringBuilder.append(", " + columnList.get(i).getName() + " " + columnList.get(i).getType());
             }
         }
         String schemaStr = stringBuilder.toString();

         TextFileFormat textFileFormat = (TextFileFormat)(dataSource.getFileFormat());
         String quote = textFileFormat.getQuote();
         String delimiter = textFileFormat.getDelimiter();

         String serdesString = "ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde' " +
         "WITH serdeproperties ('separatorChar' = '" +  delimiter + "', 'quoteChar' = '" + quote + "')";

         URI uri = dataSource.getDataSourceUri().getUri();
         String sqlCmd = "create external table " + hiveTableName + " (" + schemaStr  + ") "
         + serdesString + " STORED AS TEXTFILE LOCATION '"+ uri.toString() + "'";

        this.mDDFManager.sql(sqlCmd, this.mDDFManager.getEngine());
        DDF ddf = this.mDDFManager.sql2ddf(String.format("select * from %s", hiveTableName), this.mDDFManager
            .getEngine());

        ddf.setColumnNames(columnNames);
        return ddf;
    }
}
