package io.ddf.content;


import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 */
public class Representation {
  private Class<?>[] mTypeSpecs;
  private String mTypeSpecsString;
  private Object mValue;

  public Representation(Object value, Class<?>... typeSpecs) {
    this(typeSpecs);
    this.mValue = value;
  }

  public Representation(Object value, String typSpecString) {
    this.mValue = value;
    this.mTypeSpecsString = typSpecString;
  }

  public Representation(Class<?>... typeSpecs) {
    this.mTypeSpecs = typeSpecs;
    this.mTypeSpecsString = Representation.typeSpecsToString(typeSpecs);
  }

  public Representation(String typeSpecsString) {
    this.mTypeSpecsString = typeSpecsString;
  }

  public boolean hasValue() {
    return mValue != null;
  }

  public Object getValue() {
    return mValue;
  }

  public void setValue(Object value) {
    this.mValue = value;
  }

  public String getTypeSpecsString() {
    return this.mTypeSpecsString;
  }

  public static String typeSpecsToString(Class<?>... typeSpecs) {
    if (typeSpecs == null || typeSpecs.length == 0) return "null";

    StringBuilder sb = new StringBuilder();
    for (Class<?> c : typeSpecs) {
      sb.append(c == null ? "null" : c.getName());
      sb.append(':');
    }

    if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1); // remove last ':'

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (other == this) return true;
    if (!(other instanceof Representation)) return false;
    Representation otherMyClass = (Representation) other;
    if (otherMyClass.getTypeSpecsString().equals(this.getTypeSpecsString())) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31).append(this.getTypeSpecsString()).toHashCode();
  }
}
