package io.ddf.etl;


import com.google.gson.annotations.SerializedName;

public class Types {
  static public enum JoinType {
    @SerializedName("inner")INNER, @SerializedName("left")LEFT, @SerializedName("right")RIGHT, @SerializedName(
        "full")FULL, @SerializedName("leftsemi")LEFTSEMI;

    public String getStringRepr() throws Exception {
      if (this == LEFTSEMI) return "LEFT SEMI";
      else if (this == INNER) return "";
      else if (this == LEFT) return "LEFT OUTER";
      else if (this == RIGHT) return "RIGHT OUTER";
      else if (this == FULL) return "FULL OUTER";
      return null;
    }
  }


  ;

}
