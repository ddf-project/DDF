package io.ddf2.bigquery;

import io.ddf2.IDDFMetaData;
import io.ddf2.datasource.schema.ISchema;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * Created by sangdn on 1/21/16.
 * @see io.ddf2.IDDFMetaData
 */
public class BigQueryMetaData implements IDDFMetaData {
    @Override
    public Set<String> getAllDDFNames() {
        return null;
    }

    @Override
    public Set<Pair<String,ISchema>> getAllDDFNameWithSchema() {
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

    @Override
    public long getCreationTime(String ddfName) {
        return 0;
    }
}
