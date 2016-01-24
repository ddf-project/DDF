package io.ddf2.bigquery;

import io.ddf2.IDDFMetaData;
import io.ddf2.datasource.schema.ISchema;

import java.util.List;

/**
 * Created by sangdn on 1/21/16.
 * @see io.ddf2.IDDFMetaData
 */
public class BigQueryMetaData implements IDDFMetaData {
    @Override
    public List<String> getAllDDFNames() {
        return null;
    }

    @Override
    public List getAllDDFNameWithSchema() {
        return null;
    }

    @Override
    public ISchema getDDFSchema(String ddfName) {
        return null;
    }

    @Override
    public int dropAllDDF() {
        return 0;
    }

    @Override
    public int getNumDDF() {
        return 0;
    }

    @Override
    public boolean dropDDF(String ddfName) {
        return false;
    }
}
