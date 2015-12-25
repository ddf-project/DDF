package io.ddf.ds;

import io.ddf.DDF;
import io.ddf.exception.DDFException;

import java.util.Map;

public interface DataSource {

  DDF loadDDF(User user, Map<Object, Object> options) throws DDFException;

}
