package io.ddf.spark.util;


import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;

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
}


