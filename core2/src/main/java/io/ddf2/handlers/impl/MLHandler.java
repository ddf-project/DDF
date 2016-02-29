package io.ddf2.handlers.impl;

import io.ddf2.DDF;

public class MLHandler  implements io.ddf2.handlers.IMLHandler {
    protected DDF associatedDDF;
    public MLHandler(DDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public DDF getDDF() {
        return associatedDDF;
    }
}
 
