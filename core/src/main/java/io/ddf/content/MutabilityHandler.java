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

    String oldname = curDDF.getSchemaHandler().getTableName().replace("-", "_");

    this.getManager().sql2txt(String.format("DROP TABLE IF EXISTS %s", oldname));


    String sqlCmdNew = String
        .format(
            "CREATE TABLE %s TBLPROPERTIES (\"shark.cache\"=\"true\", \"shark.cache.storageLevel\"=\"MEMORY_AND_DISK\") AS SELECT * FROM %s",
            oldname, newddf.getTableName());
    this.getManager().sql2txt(sqlCmdNew);

    curDDF.getSchemaHandler().setSchema(newddf.getSchema());
    curDDF.getSchema().setTableName(oldname);
    return curDDF;
  }
}
