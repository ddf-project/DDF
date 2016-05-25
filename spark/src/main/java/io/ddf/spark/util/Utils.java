package io.ddf.spark.util;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters$;

public class Utils {
  public static ArrayList<String> listJars(String directory) {

    String workingDir = System.getProperty("user.dir");
    // Support relative path too
    final String completeDirectory = directory.startsWith("/")?directory:workingDir+directory;
    File dir = new File(directory);

    FilenameFilter filter = new FilenameFilter() {
      public boolean accept
          (File dir, String name) {
        // only accept jar files
        return ((new File(completeDirectory+"/"+name).list() == null )&&(name.endsWith(".jar")));
      }
    };

    String[] children = dir.list(filter);
    if (children == null) {
      System.out.println("Either dir does not exist or is not a directory");
      return null;
    }
    else {
      ArrayList<String> jars = new ArrayList<String>();
      for(String jar: children) {
        jars.add(directory+"/" + jar);
      }
      return jars;
    }
  }

  static Pattern isoPattern = Pattern.compile(
      "^(?<year>-?(?:[1-9][0-9]*)?[0-9]{4})(-(?<month>1[0-2]|0[1-9])(-"
          + "(?<day>3[01]|0[1-9]|[12][0-9])((?<sep>[T\\s])(?<hour>2[0-3]|[01][0-9])"
          + "(?<minute>:[0-5][0-9])((?<second>:[0-5][0-9])?(?<ms>\\.[0-9]+)?)?"
          + "(?<timezone>Z|\\s?[+-](?:2[0-3]|[01][0-9])(:?[0-5][0-9])?)?)?)?)?$"
  );

  public static Integer getQuarter(DateTime dateTime) {
    if(dateTime != null) {
      int month = dateTime.getMonthOfYear();
      if (month >= 1 && month <= 3) {
        return 1;
      } else if (month >= 4 && month <= 6) {
        return 2;
      } else if (month >= 7 && month <= 9) {
        return 3;
      } else if (month >= 10 && month <= 12) {
        return 4;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  public static DateTime toDateTimeObject(Object object) {
    if (object instanceof Integer) {
      // Unix timestamp
      return new DateTime((Integer) object * 1000L);
    } else if (object instanceof Long) {
      return new DateTime((Long) object * 1000L);
    } else if (object instanceof Date) {
      return new DateTime(((Date) object));
    } else if (object instanceof Timestamp) {
      return new DateTime(((Timestamp) object));
    } else if (object instanceof String) {

      Matcher matcher = isoPattern.matcher((String)object);
      if (matcher.matches()) {
        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String sep = matcher.group("sep");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String ms = matcher.group("ms");
        String timezone = matcher.group("timezone");

        StringBuilder sb = new StringBuilder();
        sb.append(year);
        sb.append(month != null ? ("-" + month) : "");
        sb.append(day != null ? ("-" + day) : "");
        sb.append(sep != null ? sep : "");
        sb.append(hour != null ? hour : "");
        sb.append(minute != null ? (minute) : "");
        sb.append(second != null ? (second) : "");
        sb.append(ms != null ? (ms) : "");
        sb.append(timezone != null ? timezone : "");

        StringBuilder sb2 = new StringBuilder();
        sb2.append("yyyy");
        sb2.append(month != null ? ("-" + "MM") : "");
        sb2.append(day != null ? ("-" + "dd") : "");
        sb2.append(sep != null ? sep.equalsIgnoreCase("T") ? "'T'" : sep : "");
        sb2.append(hour != null ? "HH" : "");
        sb2.append(minute != null ? (":" + "mm") : "");
        sb2.append(second != null ? (":" + "ss") : "");
        sb2.append(ms != null ? ("." + "SSS") : "");
        sb2.append(timezone != null ? timezone.startsWith(" ") ? " Z" : "Z" : "");

        DateTimeFormatter formatter = DateTimeFormat.forPattern(sb2.toString());
        return formatter.parseDateTime(sb.toString());
      } else {
        return null;
      }
    }
    return null;
  }

  public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
    return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
        Predef.<Tuple2<A, B>>conforms()
    );
  }
}


