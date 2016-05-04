package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleMutability extends IHandleDDFFunctionalGroup {

  @Deprecated
  public void setMutable(boolean isMutable);

  @Deprecated
  public boolean isMutable();

  public DDF updateInplace(DDF ddf) throws DDFException;
}
