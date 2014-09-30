package io.ddf.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
//import java.io.*;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.content.ISerializable;
import io.ddf.exception.DDFException;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 
 */

public class Utils {

  public static Logger sLog = LoggerFactory.getLogger(Utils.class);


  public static List<String> listFiles(String directory) {
    return listDirectory(directory, true, false);
  }

  public static List<String> listSubdirectories(String directory) {
    return listDirectory(directory, false, true);
  }

  public static List<String> listDirectory(String directory) {
    return listDirectory(directory, true, true);
  }

  @SuppressWarnings("unchecked")
  private static List<String> listDirectory(String directory, final boolean doIncludeFiles,
      final boolean doIncludeSubdirectories) {

    String[] directories = new File(directory).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(".")) return false; // HACK: auto-exclude Unix hidden files

        File item = new File(dir, name);
        if (doIncludeFiles && item.isFile()) return true;
        if (doIncludeSubdirectories && item.isDirectory()) return true;
        return false;
      }
    });

    return Arrays.asList(directories);
  }



  /**
   * Locates the given dirName as a full path, in the current directory or in successively higher parent directory
   * above.
   * 
   * @param dirName
   * @return
   * @throws IOException
   */
  public static String locateDirectory(String dirName) throws IOException {
    if (Utils.dirExists(dirName)) return dirName;

    String path = null;
    String curDir = new File(".").getCanonicalPath();

    // Go for at most 10 levels up
    for (int i = 0; i < 10; i++) {
      path = String.format("%s/%s", curDir, dirName);
      if (Utils.dirExists(path)) break;
      curDir = String.format("%s/..", curDir);
    }

    if (path != null) {
      File file = new File(path);
      path = file.getCanonicalPath();
      if (!Utils.dirExists(path)) path = null;
    }


    return path;
  }

  /**
   * Same as locateDirectory(dirName), but also creates it if it doesn't exist.
   * 
   * @param dirName
   * @return
   * @throws IOException
   */
  public static String locateOrCreateDirectory(String dirName) throws IOException {
    String path = locateDirectory(dirName);

    if (path == null) {
      File file = new File(dirName);
      file.mkdirs();
      path = file.getCanonicalPath();
    }

    return path;
  }

  /**
   * 
   * @param path
   * @return true if "path" exists and is a file (and not a directory)
   */
  public static boolean fileExists(String path) throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);
    Path filePath = new Path(path);
    return (fileSystem.exists(filePath));
  }

  public static boolean localFileExists(String path) {
    File f = new File(path);
    return (f.exists() && !f.isDirectory());
  }

  /**
   * 
   * @param path
   * @return true if "path" exists and is a directory (and not a file)
   */
  public static boolean dirExists(String path) {
    File f = new File(path);
    return (f.exists() && f.isDirectory());
  }

  public static double formatDouble(double number) {
    DecimalFormat fmt = new DecimalFormat("#.##");
    if (Double.isNaN(number)) {
      return Double.NaN;
    } else {
      return Double.parseDouble((fmt.format(number)));
    }
  }

  public static double round(double number, int precision, int mode) {
    BigDecimal bd = new BigDecimal(number);
    return bd.setScale(precision, mode).doubleValue();
  }

  public static double roundUp(double number) {
    if (Double.isNaN(number)) {
      return Double.NaN;
    } else {
      return round(number, 2, BigDecimal.ROUND_HALF_UP);
    }
  }

  public static void deleteFile(String fileName) {
    new File(fileName).delete();
  }

  public static String readFromFile(String fileName) throws IOException {
    Reader reader = null;
    Configuration configuration = new Configuration();
    try {
      FileSystem hdfs = FileSystem.get(configuration);
      FSDataInputStream inputStream = hdfs.open(new Path(fileName));
      reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
      return IOUtils.toString(reader);

    } catch (IOException ex) {
      throw new IOException(String.format("Cannot read from file %s", fileName, ex));

    } finally {
      reader.close();
    }
  }

  public static void writeToFile(String fileName, String contents) throws IOException {
    Writer writer = null;
    Configuration configuration = new Configuration();
    try {
      FileSystem hdfs = FileSystem.get(configuration);
      FSDataOutputStream outputStream = hdfs.create(new Path(fileName));

      writer = new BufferedWriter(new OutputStreamWriter(outputStream, "utf-8"));
      writer.write(contents);
      writer.close();
      hdfs.close();
    } catch (IOException ex) {
      throw new IOException(String.format("Cannot write to file %s", fileName, ex));

    } finally {
      writer.close();
    }
  }

  /**
   * 
   * @param str
   *          e.g., "a, b, c" Add a comment to this line
   * @return {"a","b","c"}
   */
  public static List<String> parseStringList(String str) {
    if (Strings.isNullOrEmpty(str)) return null;
    List<String> res = Lists.newArrayList();
    String[] segments = str.split(" *, *");
    for (String segment : segments) {
      res.add(segment.trim());
    }

    return res;
  }


  public static class JsonSerDes {

    public static final String SERDES_CLASS_NAME_FIELD = "_class";
    public static final String SERDES_TIMESTAMP_FIELD = "_timestamp";
    public static final String SERDES_USER_FIELD = "_user";


    public static String serialize(Object obj) throws DDFException {
      if (obj == null) return "null";

      if (obj instanceof ISerializable) ((ISerializable) obj).beforeSerialization();

      Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
      String json = gson.toJson(obj);

      // Add the bookkeeping fields, e.g., SERDES_CLASS_NAME_FIELD
      JsonObject jObj = toJsonObject(json);
      jObj.addProperty(SERDES_CLASS_NAME_FIELD, obj.getClass().getName());
      jObj.addProperty(SERDES_TIMESTAMP_FIELD, DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.LONG)
          .format(new Date()));
      jObj.addProperty(SERDES_USER_FIELD, System.getProperty("user.name"));


      json = gson.toJson(jObj);
      return json;
    }

    private static JsonObject toJsonObject(String json) {
      JsonElement jElement = new JsonParser().parse(json);
      if (jElement == null) return null;

      JsonObject jObj = jElement.getAsJsonObject();
      return jObj;
    }

    public static Object deserialize(String json) throws DDFException {
      if (Strings.isNullOrEmpty(json) || "null".equalsIgnoreCase(json.trim())) return null;

      try {
        JsonObject jsonObj = toJsonObject(json);
        if (jsonObj == null) return null;

        String className = jsonObj.get(SERDES_CLASS_NAME_FIELD).getAsString();
        Class<?> theClass = (!Strings.isNullOrEmpty(className) ? Class.forName(className) : JsonObject.class);

        Object obj = new Gson().fromJson(json, theClass);

        if (obj instanceof ISerializable) {
          obj = ((ISerializable) obj).afterDeserialization((ISerializable) obj, jsonObj);
        }

        return obj;

      } catch (Exception e) {
        throw new DDFException("Cannot deserialize " + json, e);
      }
    }

    public static Object loadFromFile(String path) throws DDFException {
      try {
        return deserialize(Utils.readFromFile(path));

      } catch (Exception e) {
        if (e instanceof DDFException) throw (DDFException) e;
        else throw new DDFException(e);
      }
    }
  }


  /**
   * Helper class to parse a string of the form className#methodName into an object containing the instantiated object,
   * and a method reference. It can then be used to invoke that method on that instantiated object.
   */
  public static class ClassMethod {

    private String mClassHashMethodName;
    private String mDefaultMethodName;
    private Class<?>[] mMethodArgTypes;
    private Class<?> mObjectClass;
    private Object mObject;
    private transient Method mMethod; // this is not serializable, so make it transient


    public String getClassHashMethodName() {
      return mClassHashMethodName;
    }

    public Object getObject() throws DDFException {
      if (mObject == null) mObject = this.instantiateObject(mObjectClass);
      return mObject;
    }

    public void setObject(Object obj) {
      mObject = obj;
      if (mObject != null) mObjectClass = mObject.getClass();
    }

    public Method getMethod() {
      if (mMethod == null) try {
        // Re-parse it if necessary, e.g., after serdes when we have lost mMethod since it's not serializable
        this.parse(mClassHashMethodName, mDefaultMethodName, mMethodArgTypes);

      } catch (DDFException e) {
        sLog.warn(String.format("%s: Unable to parse() in getMethod()", this.getClass().getSimpleName()), e);
      }

      return mMethod;
    }

    protected void setMethod(Method theMethod) {
      mMethod = theMethod;
    }

    public ClassMethod(String classHashMethodName, String defaultMethodName, Object... args) throws DDFException {
      this.parse(classHashMethodName, defaultMethodName, args);
    }

    public ClassMethod(String classHashMethodName, Object... args) throws DDFException {
      this.parse(classHashMethodName, null, args);
    }

    public ClassMethod(String classHashMethodName, Class<?>... argTypes) throws DDFException {
      this.parse(classHashMethodName, null, argTypes);
    }

    public ClassMethod(Object theObject, String defaultMethodName, Class<?>... argTypes) throws DDFException {
      if (theObject == null) throw new DDFException("Provided object cannot be null");

      this.setObject(theObject);
      mClassHashMethodName = String.format("%s#%s", theObject.getClass().getName(), defaultMethodName);
      this.parse(theObject.getClass(), defaultMethodName, argTypes);
    }

    private void parse(String classHashMethodName, String defaultMethodName, Object... args) throws DDFException {
      List<Class<?>> argTypes = Lists.newArrayList();

      if (args != null && args.length > 0) for (Object arg : args) {
        argTypes.add(arg == null ? Object.class : arg.getClass());
      }

      this.parse(classHashMethodName, defaultMethodName, argTypes.toArray(new Class<?>[0]));
    }

    private void parse(String classHashMethodName, String defaultMethodName, Class<?>[] argTypes) throws DDFException {
      if (Strings.isNullOrEmpty(classHashMethodName)) throw new DDFException("Class#Method name cannot be null");
      mClassHashMethodName = classHashMethodName;
      mDefaultMethodName = defaultMethodName;
      mMethodArgTypes = argTypes;

      String[] parts = mClassHashMethodName.split("#");
      if (parts.length == 1) parts = new String[] { parts[0], defaultMethodName };

      if (parts.length != 2) throw new DDFException("Invalid class#method name: " + mClassHashMethodName);

      try {
        this.parse(Class.forName(parts[0]), parts[1], argTypes);

      } catch (Exception e) {
        if (e instanceof DDFException) throw (DDFException) e;
        else throw new DDFException(e);
      }
    }

    private void parse(Class<?> theClass, String methodName, Class<?>... argTypes) throws DDFException {
      try {
        mObjectClass = theClass;
        this.findAndSetMethod(theClass, methodName, argTypes);

      } catch (Exception e) {
        throw new DDFException(String.format("Unable to parse %s#%s", theClass.getName(), methodName), e);
      }
    }

    private Object instantiateObject(Class<?> theClass) throws DDFException {
      try {
        Constructor<?> cons = null;

        try {
          cons = theClass.getDeclaredConstructor(new Class<?>[0]);
          if (cons != null) cons.setAccessible(true);

        } catch (Exception e) {
          throw new DDFException(String.format("%s needs to have a default, zero-arg constructor", theClass.getName()),
              e);
        }

        return cons.newInstance(new Object[0]);

      } catch (Exception e) {
        if (e instanceof DDFException) throw (DDFException) e;
        else throw new DDFException(e);
      }
    }

    /**
     * Allow subclasses to override this to do their own method matching logic
     * 
     * @param theClass
     * @param methodName
     * @param argTypes
     * @return
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    protected void findAndSetMethod(Class<?> theClass, String methodName, Class<?>... argTypes)
        throws NoSuchMethodException, SecurityException {

      this.setMethod(theClass.getMethod(methodName, argTypes));
    }


    public Object classInvoke(Object... args) throws DDFException {
      try {
        return this.getMethod().invoke(null, args);
      } catch (Exception e) {
        throw new DDFException(e.getCause());
      }
    }

    public Object instanceInvoke(Object... args) throws DDFException {
      if (this.getObject() == null) throw new DDFException("An object is required to invoke the given method");
      if (this.getMethod() == null) throw new DDFException("A method is required to invoke on the given object");

      try {
        return this.getMethod().invoke(this.getObject(), args);

      } catch (Exception e) {
        throw new DDFException(String.format("Error while invoking method %s on object %s", this.getMethod().getName(),
            this.getObject().getClass().getName()), e);
      }
    }
  }


  /**
   * Returns the first element, if any, of an object that may be an {@link Iterable}
   * 
   * @param maybeIterable
   * @return
   */
  public static Object getFirstElement(Object maybeIterable) {
    if (!(maybeIterable instanceof Iterable<?>)) return null;
    Iterable<?> iterable = (Iterable<?>) maybeIterable;
    Iterator<?> iterator = iterable.iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }


  public static class MethodInfo {

    private Method mMethod;
    private List<ParamInfo> mParamInfos;


    public MethodInfo(Method method) {
      mMethod = method;
      this.parse();
    }

    public Method getMethod() {
      return mMethod;
    }

    public List<ParamInfo> getParamInfos() {
      return mParamInfos;
    }

    private void parse() {
      mParamInfos = Lists.newArrayList();

      for (Type type : mMethod.getGenericParameterTypes()) {
        mParamInfos.add(new ParamInfo(type));
      }
    }


    public static class ParamInfo {
      private Type mType;
      private Type[] mTypeArgs;
      private boolean bIsParameterizedType;


      public ParamInfo(Type methodType) {
        mType = methodType;

        if (methodType instanceof ParameterizedType) {
          bIsParameterizedType = true;
          mType = ((ParameterizedType) methodType).getRawType();
          mTypeArgs = ((ParameterizedType) methodType).getActualTypeArguments();
        }
      }

      public boolean isParameterizedType() {
        return bIsParameterizedType;
      }

      public boolean argMatches(Class<?> argType) {
        return (argType.isAssignableFrom((Class<?>) mType));
      }

      public boolean argMatches(String argType) {
        return mType.toString().indexOf(argType) >= 0;
      }

      public boolean argMatches(Class<?> argType, Class<?>... paramTypes) {
        if (paramTypes == null) return this.argMatches(argType);

        return (this.argMatches(argType) && this.paramMatches(paramTypes));
      }

      public boolean argMatches(String argType, String... paramTypes) {
        if (paramTypes == null) return this.argMatches(argType);

        return (this.argMatches(argType) && this.paramMatches(paramTypes));
      }

      public boolean paramMatches(Class<?>... paramTypes) {
        for (int i = 0; i < paramTypes.length; i++) {
          sLog.info(">>>>>>>>>>>>>>>>> \t" + i + "\t" + paramTypes[i] + "\ttype=" + mTypeArgs[i]);
          try {
            if (!paramTypes[i].isAssignableFrom((Class<?>) mTypeArgs[i])) return false;
          } catch (Exception e) {
            return false;
          }
        }

        return true;
      }

      public boolean paramMatches(String... paramTypes) {
        for (int i = 0; i < paramTypes.length; i++) {
          if (mTypeArgs[i].toString().indexOf(paramTypes[0]) < 0) return false;
        }

        return true;
      }


      public Type getType() {
        return mType;
      }

      public Class<?> getTypeClass() {
        return (Class<?>) mType;
      }

      public Type[] getTypeArgs() {
        return mTypeArgs;
      }

      @Override
      public String toString() {
        if (this.isParameterizedType()) {
          return String.format("%s<%s>", this.getType(), Arrays.toString(this.getTypeArgs()));

        } else {
          return mType.toString();
        }
      }
    }
  }

}
