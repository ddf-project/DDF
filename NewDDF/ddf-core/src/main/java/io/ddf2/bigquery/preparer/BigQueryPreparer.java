package io.ddf2.bigquery.preparer;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.ViewDefinition;
import io.ddf2.bigquery.BQDataSource;
import io.ddf2.bigquery.BigQueryContext;
import io.ddf2.bigquery.BigQueryUtils;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;

import java.io.IOException;

/**
 * Created by sangdn on 1/22/16.
 * BigQueryPreparer will prepare for BigQueryDDF
 * 1/ Create VIEW from BigQueryDataSource
 * 2/ Parse respones to get Schema
 * 3/ Required:
 */
public class BigQueryPreparer implements IDataSourcePreparer {
    protected Bigquery bigquery;
    public static final String TMP_VIEW_DATASET_ID = "tmp_view_ddf";

    public BigQueryPreparer(Bigquery bigQuery) {
        this.bigquery = bigquery;


    }

    @Override
    public IDataSource prepare(String ddfName, IDataSource dataSource) throws PrepareDataSourceException {

        BQDataSource datasource = (BQDataSource) dataSource;
        assert  datasource != null;
        Table table = new Table();
        TableReference tableReference = new TableReference();
        tableReference.setTableId(ddfName);
        tableReference.setProjectId(datasource.getProjectId());
        tableReference.setDatasetId(TMP_VIEW_DATASET_ID);
        table.setTableReference(tableReference);

        ViewDefinition viewDefinition = new ViewDefinition();
        viewDefinition.setQuery(datasource.getQuery());
        table.setView(viewDefinition);
        try {
            Table tblResponse = bigquery.tables().insert(datasource.getProjectId(), TMP_VIEW_DATASET_ID, table).execute();

            return BQDataSource.builder().setProjectId(((BQDataSource) dataSource).getProjectId())
                    .setCreatedTime(tblResponse.getCreationTime())
                    .setSchema(BigQueryUtils.convertToDDFSchema(tblResponse.getSchema()))
                    .build();

        } catch (IOException e) {
            e.printStackTrace();
            throw new PrepareDataSourceException(e.getMessage());
        }
    }
}
