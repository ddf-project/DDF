package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;

public class MutabilityHandler extends ADDFFunctionalGroupHandler implements IHandleMutability {

  public MutabilityHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  private boolean isMutable = false;

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
    DDF curDDF = this.getDDF();
    curDDF.getRepresentationHandler().reset();
    curDDF.getRepresentationHandler().setRepresentations(newddf.getRepresentationHandler().getAllRepresentations());

    //    String oldname = curDDF.getSchemaHandler().getTableName();
    //
    //    this.getManager().sql2txt(String.format("DROP TABLE IF EXISTS %s", oldname));

    //must copy the factor information from curDDF to new ddf
    newddf.getMetaDataHandler().copyFactor(curDDF);

    curDDF.getSchemaHandler().setSchema(newddf.getSchema());
    return curDDF;
  }
}
  
  

