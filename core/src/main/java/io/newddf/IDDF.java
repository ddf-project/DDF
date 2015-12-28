package io.newddf;

import io.ddf.content.SqlResult;
import io.ddf.datasource.DataSourceDescriptor;

/**
 * Created by sangdn on 12/21/15.
 */
public interface IDDF {
    SqlResult sql(String query);

}
