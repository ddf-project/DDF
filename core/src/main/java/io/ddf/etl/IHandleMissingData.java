package io.ddf.etl;


import java.util.List;
import java.util.Map;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;
import io.ddf.types.AggregateTypes.AggregateFunction;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

public interface IHandleMissingData extends IHandleDDFFunctionalGroup {

  public DDF dropNA(Axis axis, NAChecking how, long thresh, List<String> columns) throws DDFException;

  public DDF fillNA(String value, FillMethod method, long limit, AggregateFunction function,
      Map<String, String> columnsToValues, List<String> columns) throws DDFException;


  public enum Axis {
    ROW, COLUMN;
    public static Axis fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (Axis t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }
  }

  public enum NAChecking {
    ANY, ALL;
    public static NAChecking fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (NAChecking t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }
  }

  public enum FillMethod {
    @SerializedName("bfill") BFILL, @SerializedName("ffill") FFILL;

    public static FillMethod fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (FillMethod t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }
  }


}
