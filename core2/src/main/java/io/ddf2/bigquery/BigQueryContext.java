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
    public static final String KEY_CLIENT_SECRET = "CLIENT-SECRET";
    public static final String KEY_REFRESH_TOKEN = "REFRESH-TOKEN";
    public static final String KEY_APP_NAME = "APP-NAME";
    public static final String KEY_PROJECT_ID = "PROJECT-ID";

    public static void setAppName(String appName) {
        DDFContext.setProperty(KEY_APP_NAME, appName);
    }

    public static String getAppName() {
        return (String) DDFContext.getProperty(KEY_APP_NAME);
    }

    public static void setClientId(String clientId){
        DDFContext.setProperty(KEY_CLIENT_ID,clientId);
    }
    public static String getClientId(){
        return (String)DDFContext.getProperty(KEY_CLIENT_ID);
    }
    public static void setClientSecret(String clientSecret){
        DDFContext.setProperty(KEY_CLIENT_SECRET,clientSecret);
    }
    public static String getClientSecret(){
        return(String)DDFContext.getProperty(KEY_CLIENT_SECRET);
    }

    public static void setRefreshToken(String refreshToken){
        DDFContext.setProperty(KEY_REFRESH_TOKEN,refreshToken);
    }
    public static String getRefreshToken(){
        return(String)DDFContext.getProperty(KEY_REFRESH_TOKEN);
    }

    public static void setProjectId(String projectId){
        DDFContext.setProperty(KEY_PROJECT_ID,projectId);
    }
    public static String getProjectId(){
        return(String)DDFContext.getProperty(KEY_PROJECT_ID);
    }




}
