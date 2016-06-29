package io.ddf.content;


import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.Map;

/**
 * <p>
 * Handles the underlying, implementation-specific representation(s) of a DDF. Note that a single DDF may have
 * simultaneously multiple representations, all of which are expected to be equivalent in terms of relevant content
 * value. Contrast this with, e.g., {@link IHandleViews}, which results in entirely new DDFs with different columns or
 * rows. These new DDFs are logically referred to as new "views".
 * </p>
 * <p>
 * For example, a DDF may initially be represented as an RDD[TablePartition]. But in order to send it to a
 * machine-learning algorithm, an RDD[LabeledPoint] representation is needed. An {@link IHandleRepresentations} is
 * expected to help perform this transformation, and it may still hold on to both representations, until they are
 * somehow invalidated.
 * </p>
 * <p>
 * In another example, a DDF may be mutated in a transformation from one RDD to a new RDD. It should automatically keep
 * track of the reference to the new RDD, so that to the client of DDF, it properly appears to have been mutated. This
 * makes it possible to for DDF to support R-style replacement functions. The underlying RDDs are of course immutable.
 * </p>
 * <p>
 * As a final example, a DDF may be set up to accept incoming streaming data. The client has a constant reference to
 * this DDF, but the underlying data is constantly being updated, such that each query against this DDF would result in
 * different data being returned/aggregated.
 * </p>
 */
public interface IHandleRepresentations extends IHandleDDFFunctionalGroup {

  /**
   * Converts a representation type specification to a unique String key
   *
   * @param typeSpecs
   * @return
   */
  String getSpecsAsString(Class<?>... typeSpecs);

  boolean has(String typeSpecs);

  boolean has(Class<?>... typeSpecs);

  /**
   * Retrieves a representation of the specified type specs.
   *
   * @param typeSpecs a variable-length array of specifications to identify the exact data representation type, e.g.,
   *                  RDD<List<Double>> would be (RDD.class, List.class, Double.class)
   * @return a pointer to the specified
   */
  Object get(Class<?>... typeSpecs) throws DDFException;

  /**
   * Retrieves a representation of the specified type specs.
   *
   * @param typeSpecs the String representation of the type specs, produced by
   *                  {@link IHandleRepresentations#getSpecsAsString(Class...)}
   * @return a pointer to the specified
   */
  Object get(String typeSpecs) throws DDFException;

  /**
   * Allows client to specify a list of acceptable type specs
   *
   * @param acceptableTypeSpecs
   * @return a representation matching one of the acceptable type specs
   */
  IGetResult get(Class<?>[][] acceptableTypeSpecs) throws DDFException;

  /**
   * Allows client to specify a list of acceptable type specs
   *
   * @param acceptableTypeSpecs
   * @return a representation matching one of the acceptable type specs
   */
  IGetResult get(String[] acceptableTypeSpecs) throws DDFException;

  /**
   * Let IHandleRepresentations knows how to convert from startRep to endRep
   */
  public void addConvertFunction(Representation fromRepresentation, Representation toRepresentation,
      ConvertFunction convertFunction);

  public void removeConvertFunction(Representation fromRepresentation, Representation toRepresentation);

  interface IGetResult {
    String getTypeSpecsString();

    Class<?>[] getTypeSpecs();

    Object getObject();
  }


  /**
   * Clears out all current representations.
   */
  void reset();

  /**
   * Clears all current representations and set it to the supplied one.
   *
   * @param data
   * @param typeSpecs a variable-length array of specifications to identify the exact data representation type, e.g.,
   *                  RDD<List<Double>> would be (RDD.class, List.class, Double.class). If not provided, the handler should do
   *                  its best to infer from the data object.
   */
  void set(Object data, Class<?>... typeSpecs);

  /**
   * Adds a representation to the set of existing representations.
   *
   * @param data
   * @param typeSpecs a variable-length array of specifications to identify the exact data representation type, e.g.,
   *                  RDD<List<Double>> would be (RDD.class, List.class, Double.class). If not provided, the handler should do
   *                  its best to infer from the data object.
   */
  void add(Object data, Class<?>... typeSpecs);

  /**
   * Removes a representation from the set of existing representations.
   *
   * @param typeSpecs a variable-length array of specifications to identify the exact data representation type, e.g.,
   *                  RDD<List<Double>> would be (RDD.class, List.class, Double.class)
   */
  void remove(Class<?>... typeSpecs);

  /**
   * Cache a representation in memory
   */
  void cache(boolean lazy);

  /**
   * Uncache all representations, e.g., in an in-memory context
   */
  void uncache(boolean lazy);

  boolean isCached();

  /**
   * Returns the default representation for this engine
   *
   * @return
   */
  Object getDefault() throws DDFException;

  void setDefaultDataType(Class<?>... typeSpecs);

  Class<?>[] getDefaultDataType();

  Map<String, Representation> getAllRepresentations();

  void setRepresentations(Map<String, Representation> reps);
}
