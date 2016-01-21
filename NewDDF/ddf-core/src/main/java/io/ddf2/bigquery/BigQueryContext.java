package io.ddf2.bigquery;

import io.ddf2.DDFContext;

/**
 * Created by sangdn on 1/20/16.
 */
public class BigQueryContext extends DDFContext {
    /**
     * Required Key need to set to DDFContext to working with BigQuery
     */
    public static final String KEY_CLIENT_ID = "CLIENT-ID";
    public static final String KEY_CLIENT_SECRET= "CLIENT-SECRET";
    public static final String KEY_REFRESH_TOKEN = "REFRESH-TOKEN";
    public static final String KEY_APP_NAME= "APP-NAME";
    public static final String KEY_PROJECT_ID= "PROJECT-ID";

}
