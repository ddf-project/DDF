package io.ddf.util;


import com.google.common.base.Strings;
import io.ddf.DDF;

import java.util.UUID;

public class DDFUtils {

  private static final int MAX_DESIRED_NAME_LEN = 60;


  /**
   * Heuristic: if the source/parent already has a table name then we can add something that identifies the operation.
   * Plus a unique extension if needed. If the starting name is already too long, we call that a degenerate case, for
   * which we go back to UUID-based. All this is overridden if the caller specifies a desired name. For the desired name
   * we still attach an extension if needed to make it unique.
   *
   * @param obj         object to be named, no hyphens
   * @param sourceName  the name of the source object, if any, based on which we will generate the name
   * @param operation   the name/brief description of the operation, if any, which we would use as an extension to the sourceName
   * @param desiredName the desired name to be used, if any
   * @return
   */
  public static String generateObjectName(Object obj, String sourceName, String operation, String desiredName) {
    if (Strings.isNullOrEmpty(desiredName)) {

      if (Strings.isNullOrEmpty(sourceName)) {
        return generateUniqueName(obj);

      } else if (Strings.isNullOrEmpty(operation)) {
        desiredName = sourceName;

      } else {
        desiredName = String.format("%s_%s", sourceName, operation);
      }

    }

    desiredName = desiredName.replace("-", "_");
    return (Strings.isNullOrEmpty(desiredName) || desiredName.length() > MAX_DESIRED_NAME_LEN) ? generateUniqueName(obj)
        : ensureUniqueness(desiredName);
  }

  public static String generateObjectName(Object obj) {
    return generateObjectName(obj, null, null, null);
  }

  public static String generateObjectName(Object obj, String sourceName) {
    return generateObjectName(obj, sourceName, null, null);
  }

  public static String generateObjectName(Object obj, String sourceName, String operation) {
    return generateObjectName(obj, sourceName, operation, null);
  }

  private static String generateUniqueName(Object obj) {
    if (obj == null) return UUID.randomUUID().toString().replace("-", "_");

    if (obj instanceof DDF) {
      return String.format("%s_%s_%s", obj.getClass().getSimpleName(), ((DDF) obj).getEngine(), UUID.randomUUID())
          .replace("-", "_");
    }

    return String.format("%s_%s", obj.getClass().getSimpleName(), UUID.randomUUID()).replace("-", "_");
  }

  private static String ensureUniqueness(String desiredName) {
    return (desiredName + UUID.randomUUID())
        .replace("-", "_"); // FIXME: this should really be done by calling into the REGISTRY subsystem
  }

  public static String saveDDFName(String ddfName) {
    return ddfName.replace("-", "_");
  }
}
