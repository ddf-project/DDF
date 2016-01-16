/**
 * Created by jing on 1/15/16.
 */

package io.ddf.rest;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

import java.util.UUID;

class RestDDFManager extends DDFManager {

    // Temp storage handler
    // Rest Client

    public DDF newDDF(String restUri) throws DDFException {
        return new RestDDF(restUri);
    }

    /**
     * @brief Get data using rest api.
     * @param restDDF
     * @return
     * @throws DDFException
     */
    public String getData(RestDDF restDDF) throws DDFException {
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
