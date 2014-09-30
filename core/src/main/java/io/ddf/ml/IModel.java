package io.ddf.ml;


/**
 */

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

public interface IModel {

  public Object predict(double[] features) throws DDFException;

  public Object getRawModel();

  public void setRawModel(Object rawModel);

  public String getName();

  public void setName(String name);

  public void setTrainedColumns(String[] columns);

  public String[] getTrainedColumns();

  public DDF serialize2DDF(DDFManager manager) throws DDFException;

  public String toJson();
}
