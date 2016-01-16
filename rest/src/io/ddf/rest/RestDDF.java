package io.ddf.rest;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

/**
 * Created by jing on 1/15/16.
 */
public class RestDDF extends DDF {
    private String restUri;


    protected RestDDF(String restUri) throws DDFException {
        this.restUri = restUri;
    }

    public String getRestUri() {
        return restUri;
    }

    public void setRestUri(String restUri) {
        this.restUri = restUri;
    }

    /**
     * @brief Filter data. Do we need this api, actually user can directly put this in uri.
     * @param filter
     */
    public void filter(String filter) {

    }

    @Override
    public DDF copy() throws DDFException {
        return null;
    }
}
