package io.spark.ddf;


import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.IHandleRepresentations.IGetResult;
import io.ddf.content.RepresentationHandler.GetResult;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

/**
 * An Apache-Spark-based implementation of DDF
 */

public class SparkDDF extends DDF {

  private static final long serialVersionUID = 7466377156065874568L;


  public SparkDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {
    super(manager);
    this.initialize(manager, data, typeSpecs, namespace, name, schema);
  }

  public <T> SparkDDF(DDFManager manager, RDD<?> rdd, Class<T> unitType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager);
    if (rdd == null) throw new DDFException("Non-null RDD is required to instantiate a new SparkDDF");
    this.initialize(manager, rdd, new Class<?>[] { RDD.class, unitType }, namespace, name, schema);
  }

  /**
   * Signature without RDD, useful for creating a dummy DDF used by DDFManager
   * 
   * @param manager
   */
  public SparkDDF(DDFManager manager) throws DDFException {
    super(manager);
  }

  /**
   * Available for run-time instantiation only.
   * 
   * @throws DDFException
   */
  protected SparkDDF() throws DDFException {
    super();
  }

  @SuppressWarnings("unchecked")
  public <T> RDD<T> getRDD(Class<T> unitType) throws DDFException {
    Object obj = this.getRepresentationHandler().get(RDD.class, unitType);
    if (obj instanceof RDD<?>) return (RDD<T>) obj;
    else throw new DDFException("Unable to get RDD with unit type " + unitType);
  }

  public IGetResult getRDD(Class<?>... acceptableUnitTypes) throws DDFException {
    if (acceptableUnitTypes == null || acceptableUnitTypes.length == 0) {
      throw new DDFException("Acceptable Unit Types must be specified");
    }

    // Compile a list of acceptableTypeSpecs
    List<Class<?>[]> acceptableTypeSpecs = new ArrayList<Class<?>[]>();
    for (Class<?> unitType : acceptableUnitTypes) {
      acceptableTypeSpecs.add(new Class<?>[] { RDD.class, unitType });
    }

    return this.getRepresentationHandler().get(acceptableTypeSpecs.toArray(new Class<?>[0][]));
  }

  public <T> JavaRDD<T> getJavaRDD(Class<T> unitType) throws DDFException {
    RDD<T> rdd = this.getRDD(unitType);
    return rdd.toJavaRDD();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public IGetResult getJavaRDD(Class<?>... acceptableUnitTypes) throws DDFException {
    IGetResult result = this.getRDD(acceptableUnitTypes);
    RDD<?> rdd = (RDD<?>) result.getObject();
    Class<?> unitType = result.getTypeSpecs()[1];

    return new GetResult(rdd.toJavaRDD(), unitType);
  }
}
