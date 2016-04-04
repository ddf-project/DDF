/**
 *
 */
package io.basic.ddf;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.ds.DataSourceCredential;
import io.ddf.exception.DDFException;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An implementation of DDFManager with local memory and local storage
 */
public class BasicDDFManager extends DDFManager {

  @Override
  public String getEngine() {
    return "basic";
  }

  public BasicDDFManager() {}

  public <T> DDF newDDF(List<T> rows, Class<T> unitType, String name, Schema schema)
      throws DDFException {

    if (rows == null || rows.isEmpty()) {
      throw new DDFException("Non-null/zero-length List is required to instantiate a new BasicDDF");
    }

<<<<<<< HEAD
    return this.newDDF(this, rows, new Class[]{List.class, unitType},
        namespace, name, schema);
=======
    return this.newDDF(this, rows, new Class[] { List.class, unitType }, name, schema);
>>>>>>> 6ab8241fdb1d1ab371de77dd68da96fa5d4a31e2
  }

  public DDF loadFile(String fileURL, String fieldSeparator) throws DDFException {
    throw new DDFException("Load DDF from file is not supported!");
  }

  @Override
  public DDF copyFrom(DDF ddf, String tgtname) throws DDFException {
    throw new DDFException("Unsupported operation");
  }

  @Override
  public DDF copyFrom(DDFManager manager, String ddfname, String tgtname) throws DDFException {
    throw new DDFException("Unsupported operation");
  }

  @Override
  public DDF copyFrom(DDF fromDDF) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }

  @Override
  public void validateCredential(DataSourceCredential credential) throws DDFException {
    // do nothing
  }

  @Override
  public String getSourceUri() {
    return "basic://";
  }

  @Override
  public DDF createDDF(Map<Object, Object> options) throws DDFException {
    throw new UnsupportedOperationException();
  }
}
