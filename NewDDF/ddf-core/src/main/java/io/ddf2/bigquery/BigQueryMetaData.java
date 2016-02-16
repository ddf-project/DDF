package io.ddf2.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableSchema;
import io.ddf2.DDFException;
import io.ddf2.IDDFMetaData;
import io.ddf2.datasource.schema.ISchema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by sangdn on 1/21/16.
 *
 * @see io.ddf2.IDDFMetaData
 */
public class BigQueryMetaData implements IDDFMetaData {
    protected Bigquery bigquery = BigQueryUtils.newInstance();

    @Override
    public Set<String> getAllDDFNames() throws DDFException {
        try {
            Set<String> ddfNames = new HashSet<>();
            DatasetList datasets = bigquery.datasets().list(BigQueryContext.getProjectId()).execute();
            for (DatasetList.Datasets dataset : datasets.getDatasets()) {
                String datasetId = dataset.getDatasetReference().getDatasetId();
                TableList tableList = bigquery.tables().list(BigQueryContext.getProjectId(), datasetId).execute();
                if (tableList.getTables() != null) {
                    for (TableList.Tables tables : tableList.getTables()) {
                        ddfNames.add(String.format("%s.%s", datasetId, tables.getTableReference().getTableId()));
                    }
                }
            }
            return ddfNames;
        } catch (Exception e) {
            throw new DDFException("Unable to getAllDDFNames msg:" + e.getMessage());
        }
    }

    @Override
    public Set<Pair<String, ISchema>> getAllDDFNameWithSchema() throws DDFException {
        try {
            Set<Pair<String, ISchema>> ddfNameAndSchema = new HashSet<>();
            DatasetList datasets = bigquery.datasets().list(BigQueryContext.getProjectId()).execute();
            for (DatasetList.Datasets dataset : datasets.getDatasets()) {
                String datasetId = dataset.getDatasetReference().getDatasetId();
                TableList tableList = bigquery.tables().list(BigQueryContext.getProjectId(), datasetId).execute();
                if (tableList.getTables() != null) {
                    for (TableList.Tables tables : tableList.getTables()) {
                        TableSchema schema = bigquery.tables().get(BigQueryContext.getProjectId(), datasetId, tables.getTableReference().getTableId()).execute().getSchema();
                        String tableName = String.format("%s.%s", datasetId, tables.getTableReference().getTableId());
                        ISchema tableSchema = BigQueryUtils.convertToDDFSchema(schema);
                        Pair<String, ISchema> tableInfo = new ImmutablePair<>(tableName, tableSchema);
                        ddfNameAndSchema.add(tableInfo);
                    }
                }
            }
            return ddfNameAndSchema;
        } catch (Exception e) {
            throw new DDFException("Unable to getAllDDFNames msg:" + e.getMessage());
        }
    }

    /**
     * @param ddfName can contain datasetId & name or just only name (default DataSetId = TMP_VIEW_DATASET_ID)
     * @return Schema of this ddfName
     */
    @Override
    public ISchema getDDFSchema(String ddfName) throws DDFException {
        try {
            String datasetId = BigQueryUtils.TMP_VIEW_DATASET_ID;
            String tableName = ddfName;
            if (ddfName.contains(".")) {
                String[] tmp = ddfName.split("\\.");
                datasetId = tmp[0];
                tableName = tmp[1];
            }
            TableSchema schema = bigquery.tables().get(BigQueryContext.getProjectId(), datasetId, tableName).execute().getSchema();
            return BigQueryUtils.convertToDDFSchema(schema);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DDFException("Unable to get Schema of " + ddfName + " msg: " + e.getMessage());
        }
    }

    @Override
    public int dropAllDDF() {
        return 0;
    }

    @Override
    public int getNumDDF() throws DDFException {
        try {
            int numDDF = 0;
            DatasetList datasets = bigquery.datasets().list(BigQueryContext.getProjectId()).execute();
            for (DatasetList.Datasets dataset : datasets.getDatasets()) {
                String datasetId = dataset.getId();
                TableList tableList = bigquery.tables().list(BigQueryContext.getProjectId(), datasetId).execute();
                numDDF += tableList.size();
            }
            return numDDF;
        } catch (Exception e) {
            throw new DDFException("Unable to getNumDDF msg:" + e.getMessage());
        }
    }

    @Override
    public boolean dropDDF(String ddfName) {
        String datasetId = BigQueryUtils.TMP_VIEW_DATASET_ID;
        String tableName = ddfName;
        if (ddfName.contains(".")) {
            String[] tmp = ddfName.split(".");
            datasetId = tmp[0];
            tableName = tmp[1];
        }
        try {
            bigquery.tables().delete(BigQueryContext.getProjectId(), datasetId, tableName).execute();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public long getCreationTime(String ddfName) throws DDFException {
        String datasetId = BigQueryUtils.TMP_VIEW_DATASET_ID;
        String tableName = ddfName;
        if (ddfName.contains(".")) {
            String[] tmp = ddfName.split(".");
            datasetId = tmp[0];
            tableName = tmp[1];
        }
        try {
            return bigquery.tables().get(BigQueryContext.getProjectId(), datasetId, tableName).execute().getCreationTime();
        } catch (IOException e) {
            throw new DDFException("Unable to getCreationTime of " + ddfName + " msg: " + e.getMessage());
        }
    }
}
