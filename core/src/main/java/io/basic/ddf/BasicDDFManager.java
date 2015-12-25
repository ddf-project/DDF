/**
 *
 */
package io.basic.ddf;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.ds.DataSourceCredential;
import io.ddf.ds.User;
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


  @Override
  public DDF transfer(UUID fromEngine, UUID ddfUUID) {
    return null;
  }

  @Override
  public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
    return null;
  }

  public BasicDDFManager() {
  }


  public <T> DDF newDDF(List<T> rows, Class<T> unitType, String namespace, String name, Schema schema)
      throws DDFException {

    if (rows == null || rows.isEmpty()) {
      throw new DDFException("Non-null/zero-length List is required to instantiate a new BasicDDF");
    }

    return this.newDDF(this, rows, new Class[] { List.class, unitType },
         namespace, name, schema);
  }

  public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
    throw new DDFException("Load DDF from file is not supported!");
  }

  @Override
  public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
    return null;
  }

  @Override
  public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
    return null;
  }

  @Override
  public DataSourceCredential addCredential(User user, Map<Object, Object> credential) {
    // do nothing, credential is not needed in basic ddf manager
    return null;
  }

  @Override
  public DDF createDDF(User user, Map<Object, Object> options) throws DDFException {
    throw new UnsupportedOperationException();
  }
}
