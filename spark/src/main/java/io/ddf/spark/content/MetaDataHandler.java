package io.ddf.spark.content;


import io.ddf.DDF;
import io.ddf.content.AMetaDataHandler;
import io.ddf.exception.DDFException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

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
    DataFrame df = (DataFrame) this.getDDF().getRepresentationHandler().get(DataFrame.class);
    return df.count();
  }
}
