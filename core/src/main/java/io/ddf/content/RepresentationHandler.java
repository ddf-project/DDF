/**
 *
 */
package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.types.AGloballyAddressable;
import io.ddf.types.IGloballyAddressable;
import org.jgrapht.GraphPath;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class RepresentationHandler extends ADDFFunctionalGroupHandler implements IHandleRepresentations {

  // The various representations for our DDF
  protected Map<String, Representation> mReps = new ConcurrentHashMap<String, Representation>();

  private RepresentationsGraph mGraph;

  public RepresentationHandler(DDF theDDF) {
    super(theDDF);
    this.mGraph = new RepresentationsGraph(this.getDDF());
  }

  public static String getKeyFor(Class<?>[] typeSpecs) {
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
  public String getSpecsAsString(Class<?>... typeSpecs) {
    return getKeyFor(typeSpecs);
  }

  /**
   * Gets an existing representation for our {@link DDF} matching the given dataType, if any.
   *
   * @param typeSpecs
   * @return null if no matching representation available
   */
  @Override
  public Object get(Class<?>... typeSpecs) throws DDFException {
    //    return this.get(this.getSpecsAsString(typeSpecs), true);
    return this.get(Representation.typeSpecsToString(typeSpecs), true);
  }


  @Override
  public Object get(String typeSpecs) throws DDFException {
    return this.get(typeSpecs, true);
  }

  @Override
  public IGetResult get(Class<?>[][] acceptableTypeSpecs) throws DDFException {
    if (acceptableTypeSpecs == null || acceptableTypeSpecs.length == 0) return null;

    // First check what formats are *already* there, so we can save the effort of having to create it
    for (Class<?>[] ats : acceptableTypeSpecs) {
      if (this.has(ats)) return new GetResult(this.get(ats), ats);
    }

    // Now see which formats can be created
    for (Class<?>[] ats : acceptableTypeSpecs) {
      Object result = this.get(ats);
      if (result != null) return new GetResult(result, ats);
    }

    return null;
  }

  @Override
  public IGetResult get(String[] acceptableTypeSpecs) throws DDFException {
    if (acceptableTypeSpecs == null || acceptableTypeSpecs.length == 0) return null;

    // First check what formats are *already* there, so we can save the effort of having to create it
    for (String ats : acceptableTypeSpecs) {
      if (this.has(ats)) return new GetResult(this.get(ats, false), ats);
    }

    // Now see which formats can be created
    for (String ats : acceptableTypeSpecs) {
      Object result = this.get(ats, true);
      if (result != null) return new GetResult(result, ats);
    }

    return null;
  }

  private Object get(String typeSpecs, boolean doCreate) throws DDFException {

    if (this.mReps.isEmpty()) {
      return null;
    }
    Representation obj = mReps.get(typeSpecs);

    if (obj == null && doCreate) {
      Representation representation = new Representation(typeSpecs);
      try {
        obj = this.createRepresentation(representation);
      } catch (Exception e) {
        throw new DDFException(String.format("Error creating representation %s", typeSpecs), e);
      }
      if (obj != null) mReps.put(typeSpecs, obj);
    }
    if (obj != null) {
      return obj.getValue();
    } else {
      return null;
    }
  }

  @Override
  public void addConvertFunction(Representation fromRepresentation, Representation toRepresentation,
      ConvertFunction convertFunction) {
    this.mGraph.addEdge(fromRepresentation, toRepresentation, convertFunction);
  }

  @Override
  public void removeConvertFunction(Representation fromRepresentation, Representation toRepresentation) {
    this.mGraph.removeEdge(fromRepresentation, toRepresentation);
  }

  @Override
  public boolean has(String typeSpecs) {
    return mReps.containsKey(typeSpecs);
  }

  @Override
  public boolean has(Class<?>... typeSpecs) {
    return this.has(this.getSpecsAsString(typeSpecs));
  }


  private Class<?>[] mDefaultTypeSpecs;


  /**
   * Returns the default dataType for this engine. The base implementation returns Object[][].class.
   *
   * @return
   */
  @Override
  public Class<?>[] getDefaultDataType() {
    return mDefaultTypeSpecs;
  }

  @Override
  public void setDefaultDataType(Class<?>... typeSpecs) {
    mDefaultTypeSpecs = typeSpecs;
  }

  @Override
  public Object getDefault() throws DDFException {
    return this.get(this.getDefaultDataType());
  }

  /**
   * Resets (or clears) all representations
   */
  @Override
  public void reset() {
    this.uncacheAll();
    mReps.clear();
    this.setDefaultDataType((Class<?>[]) null);
  }

  private boolean equalsDefaultDataType(Class<?>... typeSpecs) {
    return this.getSpecsAsString(typeSpecs).equals(this.getSpecsAsString(this.getDefaultDataType()));
  }


  /**
   * Converts from existing representation(s) to the desired representation, which has the specified dataType.
   * <p/>
   * The base representation returns only the default representation if the dataType matches the default type. Otherwise
   * it returns null.
   *
   * @param dataType
   * @return
   */
  private Representation createRepresentation(Representation representation) throws DDFException {

    Collection<Representation> vertices = this.mReps.values();
    //List<GraphPath<Representation<?>, ConvertFunction<?, ?>>> pathList = new ArrayList<GraphPath<Representation<?>, ConvertFunction<?, ?>>>();
    double minWeight = Double.MAX_VALUE;
    GraphPath<Representation, ConvertFunction> minPath = null;
    for (Representation vertex : vertices) {
      mLog.info(">>>>>> start vertex = " + vertex.getTypeSpecsString());
      mLog.info(">>>>>> end Vertex = " + representation.getTypeSpecsString());
      GraphPath<Representation, ConvertFunction> shortestPath = this.mGraph.getShortestPath(vertex, representation);

      if (shortestPath != null) {
        mLog.info(">>>> shortestPath != null");
        if (shortestPath.getWeight() < minWeight) {
          minWeight = shortestPath.getWeight();
          minPath = shortestPath;
        }
      }
    }

    if (minPath == null) {
      return null;
    } else {
      Representation startVertex = minPath.getStartVertex();
      Representation startRepresentation = null;
      for (Representation rep : this.mReps.values()) {
        if (rep.equals(startVertex)) {
          startRepresentation = rep;
        }
      }
      mLog.info("minPath.getWeight = " + minPath.getWeight());
      mLog.info("minPath.lenth = " + minPath.getEdgeList().size());
      mLog.info("startVertext = " + minPath.getStartVertex().getTypeSpecsString());
      mLog.info("endVertex = " + minPath.getEndVertex().getTypeSpecsString());
      List<ConvertFunction> convertFunctions = minPath.getEdgeList();
      Representation objectRepresentation = startRepresentation;
      for (ConvertFunction func : convertFunctions) {
        objectRepresentation = func.apply(objectRepresentation);
      }
      return objectRepresentation;
    }
  }

  public static Class<?>[] determineTypeSpecs(Object data, Class<?>... typeSpecs) {
    if (typeSpecs != null && typeSpecs.length > 0) return typeSpecs;
    return (data == null ? null : new Class<?>[] { data.getClass() });
  }

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   */
  @Override
  public void set(Object data, Class<?>... typeSpecs) {
    this.reset();
    this.mDefaultTypeSpecs = typeSpecs;
    this.add(data, typeSpecs);
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones but replacing the one that
   * matches the given DDFManagerType, dataType tuple.
   */
  @Override
  public void add(Object data, Class<?>... typeSpecs) {
    //    if (data == null) return;
    //
    //    typeSpecs = determineTypeSpecs(data, typeSpecs);
    //    if (this.getDefaultDataType() == null) this.setDefaultDataType(typeSpecs);
    //
    //    mReps.put(this.getSpecsAsString(typeSpecs), data);
    Representation representation = new Representation(data, typeSpecs);
    mReps.put(representation.getTypeSpecsString(), representation);
  }

  /**
   * Removes a representation from the set of existing representations.
   *
   * @param dataType
   */
  @Override
  public void remove(Class<?>... typeSpecs) {
    mReps.remove(this.getSpecsAsString(typeSpecs));
    //if (this.equalsDefaultDataType(typeSpecs)) this.reset();
  }

  /**
   * Returns a String list of current representations, useful for debugging
   */
  public String getList() {
    String result = "";
    int i = 1;

    for (String s : mReps.keySet()) {
      result += (i++) + ". key='" + s + "', value='" + mReps.get(s) + "'\n";
    }

    return result;
  }

  @Override
  public void cleanup() {
    mReps.clear();
    super.cleanup();
    uncacheAll();
  }

  @Override
  public void cache(boolean lazy) {

  }

  @Override
  public void cacheAll() {
    // TODO Auto-generated method stub

  }

  @Override
  public void uncacheAll() {
    // TODO Auto-generated method stub
  }


  /**
   * A special class representing a Table that's native to the engine, e.g., Shark Table for the Spark engine.
   */
  public static class NativeTable extends AGloballyAddressable {
    private String mEngineName;
    private String mNamespace;
    private String mName;


    public NativeTable(String namespace, String name) {
      mNamespace = namespace;
      mName = name;
    }

    @Override
    public String getEngineName() {
      return mEngineName;
    }

    @Override
    public void setEngineName(String mEngineName) {
      this.mEngineName = mEngineName;
    }

    @Override
    public String getNamespace() {
      return mNamespace;
    }

    @Override
    public void setNamespace(String namespace) {
      mNamespace = namespace;
    }

    @Override
    public String getName() {
      return mName;
    }

    public void setName(String name) {
      mName = name;
    }

    @Override
    public String getGlobalObjectType() {
      return "native_table";
    }
  }


  public static final String NATIVE_TABLE = getKeyFor(new Class<?>[] { NativeTable.class });


  public static class GetResult implements IGetResult {

    public GetResult(Object obj, Class<?>... typeSpecs) {
      this(obj, RepresentationHandler.getKeyFor(typeSpecs));
      mTypeSpecs = typeSpecs;
    }


    /**
     * Internal use only. Don't encourage external use, since we want to require the Class<?>... typeSpecs format.
     *
     * @param obj
     * @param typeSpecsString
     */
    private GetResult(Object obj, String typeSpecsString) {
      mObject = obj;
      mTypeSpecsString = typeSpecsString;
    }


    private Class<?>[] mTypeSpecs;
    private String mTypeSpecsString;
    private Object mObject;


    @Override
    public Class<?>[] getTypeSpecs() {
      return mTypeSpecs;
    }

    @Override
    public String getTypeSpecsString() {
      return mTypeSpecsString;
    }

    @Override
    public Object getObject() {
      return mObject;
    }
  }


  @Override
  public Map<String, Representation> getAllRepresentations() {
    return mReps;
  }

  @Override
  public void setRepresentations(Map<String, Representation> reps) {
    mReps.clear();
    mReps.putAll(reps);
  }

}
