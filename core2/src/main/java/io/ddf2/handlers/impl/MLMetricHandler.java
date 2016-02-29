package io.ddf2.handlers.impl;


import io.ddf2.DDF;
import io.ddf2.handlers.IDDFHandler;
import io.ddf2.handlers.IMLMetricHandler;

public class MLMetricHandler implements IMLMetricHandler{
    protected DDF associatedDDF;
    public MLMetricHandler(DDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public DDF getDDF() {
        return associatedDDF;
    }
}
 
