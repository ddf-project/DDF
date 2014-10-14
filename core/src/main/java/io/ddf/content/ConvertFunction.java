package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;

import java.io.Serializable;

/**
 */

public abstract class ConvertFunction implements Serializable {

  private transient DDF mDDF;

  public ConvertFunction(DDF ddf) {
    this.mDDF = ddf;
  }

  public abstract Representation apply(Representation rep) throws DDFException;
}
