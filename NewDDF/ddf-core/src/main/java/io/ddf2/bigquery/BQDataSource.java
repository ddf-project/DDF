package io.ddf2.bigquery;

import io.ddf2.datasource.DataSource;
import io.ddf2.datasource.SqlDataSource;

/**
 * Created by sangdn on 1/22/16.
 * BigQueryDataSource provides datasource info for BigQueryDDF
 * + sqlQuery : sql query to execute in BigQuery
 * + projectId: required param from BigQuery library.
 */
public class BQDataSource extends DataSource {
    protected String projectId;
    protected String query;


    protected BQDataSource() {
    }

    public String getProjectId() {
        return projectId;
    }

    public String getQuery() {
        return query;
    }

    public static Builder<BQDataSource> builder() {
        return new Builder<BQDataSource>() {
            @Override
            protected BQDataSource newInstance() {
                return new BQDataSource();
            }
        };
    }

    public static abstract class Builder<T extends BQDataSource> extends DataSource.Builder<T> {
        public Builder<T> setProjectId(String projectId) {
            this.datasource.projectId = projectId;
            return this;
        }
        public Builder<T> setQuery(String query){
            this.datasource.query = query;
            return this;
        }
    }

}
