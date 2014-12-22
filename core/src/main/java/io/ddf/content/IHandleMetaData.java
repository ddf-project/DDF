package io.ddf.content;


import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.UUID;

public interface IHandleMetaData extends IHandleDDFFunctionalGroup {

  public UUID getId();

  public void setId(UUID id);

  public long getNumRows() throws DDFException;

}
