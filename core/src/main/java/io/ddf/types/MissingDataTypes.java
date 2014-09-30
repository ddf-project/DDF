package io.ddf.types;


import com.google.common.base.Strings;

public class MissingDataTypes {
  /**
   * Abstract base type for support of missing values (NAs) in the data
   * 
   * @author ctn
   * 
   */
  public abstract class AMissingValue<T> {
    protected T mValue;


    public T getValue() {
      return mValue;
    }

    public void setValue(T value) {
      mValue = value;
    }

    public boolean isNA() {
      return mValue == null;
    }

    public boolean isMissing() {
      return this.isNA();
    }

    protected String toStringWhenNotNull() {
      return mValue.toString();
    }

    @Override
    public String toString() {
      return mValue != null ? this.toStringWhenNotNull() : "NA";
    }
  }

  public class DoubleOrNA extends AMissingValue<Double> {}

  public class IntegerOrNA extends AMissingValue<Integer> {}

  public class LongOrNA extends AMissingValue<Long> {}

  public class FloatOrNA extends AMissingValue<Float> {}

  public class ObjectOrNA extends AMissingValue<Object> {}

  public class NA extends AMissingValue<Byte> {

    @Override
    protected String toStringWhenNotNull() {
      return "NA";
    }

    @Override
    public void setValue(Byte value) {
      // Do not allow setting to anything other than null
      mValue = null;
    }
  }

  public enum NAString {
    NAN, NA, NONE, NULL;

    public static NAString fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (NAString t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }
  }
}
