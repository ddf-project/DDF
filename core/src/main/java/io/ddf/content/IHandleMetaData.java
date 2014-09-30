package io.ddf.content;


import java.util.UUID;
import io.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleMetaData extends IHandleDDFFunctionalGroup {

  public UUID getId();

  public void setId(UUID id);

  public long getNumRows();

}
