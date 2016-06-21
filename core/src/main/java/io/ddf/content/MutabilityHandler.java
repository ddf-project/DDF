package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;

public class MutabilityHandler extends ADDFFunctionalGroupHandler implements IHandleMutability {

  public MutabilityHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  private boolean isMutable = true;

  @Override
  public boolean isMutable() {
    return isMutable;
  }

  @Override
  public void setMutable(boolean isMutable) {
    this.isMutable = isMutable;
  }

  @Override
  public DDF updateInplace(DDF newddf) throws DDFException {
    //copy content of newddf to this ddf
    DDF curDDF = this.getDDF();
    //cache the new representation if the current ddf is cached
    if(curDDF.getRepresentationHandler().isCached()) {
      newddf.getRepresentationHandler().cache(false);
    }
    curDDF.getRepresentationHandler().reset();
    curDDF.getRepresentationHandler().setRepresentations(newddf.getRepresentationHandler().getAllRepresentations());
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    curDDF.getSchemaHandler().setSchema(newddf.getSchema());
    return curDDF;
  }
}
  
  

