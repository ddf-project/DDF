package io.ddf2.bigquery;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import io.ddf2.datasource.DataSource;
import io.ddf2.datasource.SqlDataSource;

/**
 * Created by sangdn on 1/19/16.
 */
public class BigQueryDS extends DataSource{


    protected GoogleCredential credential;
    protected String sql;
    public BigQueryDS(String sql,GoogleCredential credential){
        this.sql = sql;
        this.credential = credential;
    }
    public BigQueryDS(String sql,String clientId,String secret,String refreshToken){
        this(sql, BigQueryUtils.makeCredential(clientId, secret, refreshToken));
    }

    public GoogleCredential getCredential() {
        return credential;
    }
    public String getSql() {
        return sql;
    }

}
