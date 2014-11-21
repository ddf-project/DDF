package io.spark.ddf.content;


import io.ddf.DDF;
import io.ddf.content.AMetaDataHandler;
import org.apache.log4j.Logger;

import java.util.List;

/**
 *
 */
public class MetaDataHandler extends AMetaDataHandler {
  private static Logger logger = Logger.getLogger(MetaDataHandler.class);


  public MetaDataHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  protected long getNumRowsImpl() {
    String tableName = this.getDDF().getSchemaHandler().getTableName();
    logger.debug("get NumRows Impl called");
    try {
      List<String> rs = this.getManager().sql2txt("SELECT COUNT(*) FROM " + tableName);
      return Long.parseLong(rs.get(0));
    } catch (Exception e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return 0;
  }

}
