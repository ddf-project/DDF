/**
 *
 */
package io.basic.ddf;


import io.ddf.DDF;

import java.lang.reflect.Array;
import java.util.List;

/**
 *
 */
public class RepresentationHandler extends io.ddf.content.RepresentationHandler {

  public RepresentationHandler(DDF theDDF) {
    super(theDDF);
  }


  /**
   * Supported Representations
   */
  public static final String LIST_ARRAY_DOUBLE = getKeyFor(new Class<?>[] { List.class, Array.class, Double.class });
  public static final String LIST_ARRAY_OBJECT = getKeyFor(new Class<?>[] { List.class, Array.class, Object.class });


  @Override
  public Class<?>[] getDefaultDataType() {
    return new Class<?>[] { List.class, Array.class, Object.class };
  }


}
