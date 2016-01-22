package io.ddf2.bigquery;

import io.ddf2.datasource.SqlDataSource;

/**
 * Created by sangdn on 1/22/16.
 * BigQueryDataSource provides datasource info for BigQueryDDF
 * + sqlQuery : sql query to execute in BigQuery
 * + projectId: required param from BigQuery library.
 */
public class BQDataSource extends SqlDataSource {
    protected String projectId;
    public BQDataSource(String sqlQuery) {
        this((String)BigQueryContext.getProperty(BigQueryContext.KEY_PROJECT_ID),sqlQuery);
    }
    public BQDataSource(String projectId,String sqlQuery){
        super(sqlQuery);
        this.projectId = projectId;
    }

    public String getProjectId() {
        return projectId;
    }



}
