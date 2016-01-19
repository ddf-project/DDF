package io.ddf2.bigquery;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

/**
 * Created by sangdn on 1/19/16.
 */
public class BigQueryUtils {

    /***
     *
     * @param appName: Application Name to submit to Google Bigquery Service
     * @param clientId: ClientId to connect to Google BigQuery Service
     * @param clientSecret: Client Secret
     * @param refreshToken: Refresh Token
     * @return
     */
    public static Bigquery newInstance(String appName,String clientId,String clientSecret,String refreshToken){

        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = new GoogleCredential.Builder().setClientSecrets(clientId, clientSecret)
                .setTransport(transport).setJsonFactory(jsonFactory).build();
        credential.setRefreshToken(refreshToken);


        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName(appName).build();
    }

    public static GoogleCredential makeCredential(String clientId, String secrect, String refreshToken) {
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = new GoogleCredential.Builder().setClientSecrets(clientId, secrect)
                .setTransport(transport).setJsonFactory(jsonFactory).build();
        credential.setRefreshToken(refreshToken);


        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return credential;
    }
}
