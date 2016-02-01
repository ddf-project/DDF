package io.ddf2.handlers.impl;


import io.ddf2.IDDF;
import io.ddf2.handlers.IDDFHandler;
import io.ddf2.handlers.IMLMetricHandler;

public class MLMetricHandler implements IMLMetricHandler{
    protected IDDF associatedDDF;
    public MLMetricHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }
}
 
