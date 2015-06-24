package io.ddf.spark.content;


import io.ddf.DDF;
import io.ddf.content.AMetaDataHandler;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import org.apache.log4j.Logger;
import io.ddf.exception.DDFException;
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
  protected long getNumRowsImpl() throws DDFException {
    String tableName = this.getDDF().getSchemaHandler().getTableName();
    logger.debug("get NumRows Impl called");
    try {
      List<String> rs = this.getManager().sql2txt("SELECT COUNT(*) FROM " + tableName).getRows();
      return Long.parseLong(rs.get(0));
    } catch (Exception e) {
      throw new DDFException("Error getting NRow", e);
    }
  }
}
