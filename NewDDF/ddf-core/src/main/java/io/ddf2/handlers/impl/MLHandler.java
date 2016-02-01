package io.ddf2.handlers.impl;

import io.ddf2.IDDF;

public class MLHandler  implements io.ddf2.handlers.IMLHandler {
    protected IDDF associatedDDF;
    public MLHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }
}
 
