package io.ddf2.bigquery.preparer;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import io.ddf2.bigquery.BQDataSource;
import io.ddf2.bigquery.BigQueryContext;
import io.ddf2.bigquery.BigQueryUtils;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sangdn on 1/22/16.
 * BigQueryPreparer will prepare for BigQueryDDF
 * 1/ Create VIEW from BigQueryDataSource
 * 2/ Parse respone to get Schema
 * 3/ Required:
 */
public class BigQueryPreparer implements IDataSourcePreparer {
    protected Bigquery bigquery;

    //ToDo: Move isFirstTime & hasTmpDataset to ThreadLocal
    public static final AtomicBoolean isFirstTime = new AtomicBoolean(true);
    public static final AtomicBoolean hasTmpDataSet = new AtomicBoolean(false);
    public BigQueryPreparer(Bigquery bigquery) {
        this.bigquery = bigquery;
//        removeOldView();


    }

    private void removeOldView() {
        try{
            if(isFirstTime.get()==true){
                synchronized (isFirstTime){
                    if (isFirstTime.get()==true){
                        bigquery.datasets().delete(BigQueryContext.getProjectId(), BigQueryUtils.TMP_VIEW_DATASET_ID).execute();
                        isFirstTime.set(false);
                    }
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    public IDataSource prepare(String ddfName, IDataSource dataSource) throws PrepareDataSourceException {
        try {
            ensureTmpViewDataSet();
            BQDataSource datasource = (BQDataSource) dataSource;
            assert datasource != null;
            Table table = new Table();
            TableReference tableReference = new TableReference();
            tableReference.setTableId(ddfName);
            tableReference.setProjectId(datasource.getProjectId());
            tableReference.setDatasetId(BigQueryUtils.TMP_VIEW_DATASET_ID);

            table.setTableReference(tableReference);

            ViewDefinition viewDefinition = new ViewDefinition();
            viewDefinition.setQuery(datasource.getQuery());
            table.setView(viewDefinition);
            long expirationTime = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
            table.setExpirationTime(expirationTime);

            Table tblResponse = bigquery.tables().insert(datasource.getProjectId(), BigQueryUtils.TMP_VIEW_DATASET_ID, table).execute();

            return BQDataSource.builder().setProjectId(((BQDataSource) dataSource).getProjectId())
                    .setNumRows(tblResponse.getNumRows().longValue())
                    .setDatasetId(BigQueryUtils.TMP_VIEW_DATASET_ID)
                    .setCreatedTime(tblResponse.getCreationTime())
                    .setSchema(BigQueryUtils.convertToDDFSchema(tblResponse.getSchema()))
                    .build();

        } catch (IOException e) {
            e.printStackTrace();
            throw new PrepareDataSourceException(e.getMessage());
        }
    }

    /**
     * Ensure TempViewDataSet has created on current projectId
     * @throws IOException
     * @throws PrepareDataSourceException
     */
    private void ensureTmpViewDataSet() throws IOException, PrepareDataSourceException {
        if(hasTmpDataSet.get() == false){
            synchronized (hasTmpDataSet){
                if(hasTmpDataSet.get()==false){
                    Boolean isFound  = false;
                    DatasetList list = bigquery.datasets().list(BigQueryContext.getProjectId()).execute();
                    for (DatasetList.Datasets ds :list.getDatasets()){
                        String datasetId = ds.getDatasetReference().getDatasetId();
                        if(datasetId.equals(BigQueryUtils.TMP_VIEW_DATASET_ID)){
                            isFound = true; break;
                        }
                    }
                    if(isFound == false){
                        DatasetReference ref = new DatasetReference();
                        ref.setDatasetId(BigQueryUtils.TMP_VIEW_DATASET_ID);
                        Dataset dataset = new Dataset();
                        dataset.setDatasetReference(ref);
                        dataset = bigquery.datasets()
                                .insert(BigQueryContext.getProjectId(),dataset).execute();
                        if(dataset == null || dataset.getDatasetReference().getDatasetId().equals(BigQueryUtils.TMP_VIEW_DATASET_ID) == false){
                            throw new PrepareDataSourceException("Couldn't Create Temprory Dataset to store View");
                        }
                    }
                    hasTmpDataSet.set(true);
                }
            }
        }


    }
}
