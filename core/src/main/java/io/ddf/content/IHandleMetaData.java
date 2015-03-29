package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.UUID;

public interface IHandleMetaData extends IHandleDDFFunctionalGroup {

  public UUID getId();

  public void setId(UUID id);

  public long getNumRows() throws DDFException;

  public void copyFactor(DDF ddf)  throws DDFException;

  public void copyMetaData(DDF ddf) throws DDFException;

  // return true if ddf is in use
  // false otherwise
  public boolean inUse();

  //increase number of user using ddf
  public void increaseUseCount();
}
