package io.ddf.jdbc;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

/**
 * Created by freeman on 7/17/15.
 */
public class JDBCDDF extends DDF {

  public JDBCDDF(DDFManager manager, String namespace, String name, Schema schema) throws DDFException {
    super(manager, null, null, namespace, name, schema);
  }

  public JDBCDDF(DDFManager manager) throws DDFException {
    super(manager);
  }

  public JDBCDDF() throws DDFException {
  }
}
