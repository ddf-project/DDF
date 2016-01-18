/**
 * Created by jing on 1/15/16.
 */

package io.ddf.rest;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;

class RestDDFManager extends DDFManager {

    // Temp storage handler
    // Rest Client
    Client client = ClientBuilder.newClient();

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
        WebTarget target = client.target(restDDF.getRestUri());
        Invocation.Builder request = target.request();
        request.accept(MediaType.APPLICATION_JSON_TYPE);
        Response response = request.get();
        if (response.getStatus() != 200) {

        } else {
            System.out.println(response.getEntity());
        }
        return response.getEntity().toString();
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
