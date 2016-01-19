/**
 * Created by jing on 1/15/16.
 */

package io.ddf.rest;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;


import java.io.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

class RestDDFManager extends DDFManager {

    // Temp storage handler
    // Rest Client
    //Client client = ClientBuilder.newClient();
    HttpClient httpClient = new DefaultHttpClient();

    public RestDDF newDDF(String restUri) throws DDFException {
        return new RestDDF(restUri);
    }

    /**
     * @brief Get data using rest api.
     * @param restDDF
     * @return
     * @throws DDFException
     */
    public String getData(RestDDF restDDF) throws DDFException {
        /*
        WebTarget target = client.target(restDDF.getRestUri());
        Invocation.Builder request = target.request();
        request.accept(MediaType.APPLICATION_JSON_TYPE);
        Response response = request.get();
        if (response.getStatus() != 200) {

        } else {
            System.out.println(response.getEntity());
        }
        return response.getEntity().toString();
        */
        try {
            HttpGet httpGetRequest = new HttpGet(restDDF.getRestUri());
            HttpResponse httpResponse = httpClient.execute(httpGetRequest);

            System.out.println("----------------------------------------");
            System.out.println(httpResponse.getStatusLine());
            System.out.println("----------------------------------------");

            HttpEntity entity = httpResponse.getEntity();

            byte[] buffer = new byte[1024];
            if (entity != null) {
                InputStream inputStream = entity.getContent();
                try {
                    int bytesRead = 0;
                    BufferedInputStream bis = new BufferedInputStream(inputStream);
                    while ((bytesRead = bis.read(buffer)) != -1) {
                        String chunk = new String(buffer, 0, bytesRead);
                        System.out.println(chunk);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try { inputStream.close(); } catch (Exception ignore) {}
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
        return null;
    }



    @Override
    public DDF transfer(UUID fromEngine, UUID ddfuuid) throws DDFException {
        return null;
    }

    @Override
    public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
        return null;
    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        return null;
    }

    @Override
    public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
        return null;
    }

    @Override
    public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
        return null;
    }

    @Override
    public String getEngine() {
        return "rest";
    }
}
