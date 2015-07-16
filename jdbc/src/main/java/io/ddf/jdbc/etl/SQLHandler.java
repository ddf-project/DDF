package io.ddf.jdbc.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataFormat;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;

/**
 * Created by freeman on 7/15/15.
 */
public class SQLHandler extends ASqlHandler {
  public SQLHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override public DDF sql2ddf(String command) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return null;
  }

  @Override public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat)
      throws DDFException {
    return null;
  }

  @Override public SqlResult sql(String command) throws DDFException {
    return null;
  }

  @Override public SqlResult sql(String command, Integer maxRows) throws DDFException {
    return null;
  }

  @Override public SqlResult sql(String command, Integer maxRows, String dataSource) throws DDFException {
    return null;
  }
}
