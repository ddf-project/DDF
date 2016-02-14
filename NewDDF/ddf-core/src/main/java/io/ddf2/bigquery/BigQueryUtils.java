package io.ddf2.bigquery;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.Schema;

import java.util.List;

/**
 * Created by sangdn on 1/19/16.
 */
public class BigQueryUtils {
    public static final String TMP_VIEW_DATASET_ID = "tmp_view_ddf";

    /***
     * @param appName:      Application Name to submit to Google Bigquery Service
     * @param clientId:     ClientId to connect to Google BigQuery Service
     * @param clientSecret: Client Secret
     * @param refreshToken: Refresh Token
     * @return
     */
    public static Bigquery newInstance(String appName, String clientId, String clientSecret, String refreshToken) {

        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = new GoogleCredential.Builder()
                .setClientSecrets(clientId, clientSecret)
                .setTransport(transport)
                .setJsonFactory(jsonFactory)
                .build();
        credential.setRefreshToken(refreshToken);


        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName(appName).build();
    }

    /**
     * @return BigQuery from current user setting on BigQueryContext.
     */
    public static Bigquery newInstance() {
        return newInstance(BigQueryContext.getAppName(),
                BigQueryContext.getClientId(),
                BigQueryContext.getClientSecret(),
                BigQueryContext.getRefreshToken());
    }

    public static GoogleCredential makeCredential(String clientId, String secret, String refreshToken) {
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = new GoogleCredential.Builder().setClientSecrets(clientId, secret)
                .setTransport(transport).setJsonFactory(jsonFactory).build();
        credential.setRefreshToken(refreshToken);


        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return credential;
    }

    /**
     * ref: https://cloud.google.com/bigquery/preparing-data-for-bigquery?hl=en#datatypes
     *
     * @param type
     * @return
     */
    public static Class convertToJavaType(String type) {
        type = type.toLowerCase();
        switch (type) {
            case "string":
                return String.class;
            case "integer":
                return Long.class;
            case "float":
                return Double.class;
            case "boolean":
                return Boolean.class;
            case "record":
                return List.class;
            case "timestamp":
                return DateTime.class;
            default:
                return Object.class;
        }
    }


    public static ISchema convertToDDFSchema(TableSchema tableSchema) {
        List<TableFieldSchema> fields = tableSchema.getFields();
        Schema.SchemaBuilder builder = Schema.builder();
        for (TableFieldSchema field : fields) {
            builder.add(field.getName(), BigQueryUtils.convertToJavaType(field.getType()));
        }
        return builder.build();
    }
}
