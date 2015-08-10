/**
 *
 */
package io.basic.ddf;


import com.google.common.base.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import io.basic.ddf.content.PersistenceHandler.BasicPersistible;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.ISerializable;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

import java.util.List;

/**
 * An implementation of DDF with local memory and local storage
 */
public class BasicDDF extends DDF {

  private static final long serialVersionUID = -4318701865079177594L;

  protected static final BasicDDFManager sDummyBasicDDFManager = new BasicDDFManager();


  private Class<?> mUnitType;
  @Expose private List<?> mData; // only needed during serialization
  @Expose private String mUnitTypeName; // only needed during serialization

  public BasicDDF(List<?> rows, Class<?> unitType, String engineName, String
          namespace, String name, Schema schema) throws DDFException {
    this((DDFManager) null, (List<?>) rows, unitType, engineName, namespace,
            name,
            schema);
    if (rows != null) mUnitType = unitType;
  }

  public BasicDDF(DDFManager manager, List<?> rows, Class<?> unitType,
                  String engineName, String namespace, String name, Schema
                          schema)
      throws DDFException {

    super(manager, sDummyBasicDDFManager);
    if (rows == null) throw new DDFException("Non-null rows List is required to instantiate a new BasicDDF");
    mUnitType = unitType;
    this.initialize(manager, rows, new Class[] { List.class, unitType },
            engineName, namespace, name, schema);
  }

  /**
   * This signature is needed to support {@link DDFManager#newDDF(DDFManager,
   * Object, Class[], String, String, String, Schema)}
   *
   * @param manager
   * @param rows
   * @param typeSpecs
   * @param namespace
   * @param name
   * @param schema
   * @throws DDFException
   */
  public BasicDDF(DDFManager manager, Object rows, Class<?>[] typeSpecs,
                  String engineName, String namespace, String name, Schema
                          schema)
      throws DDFException {

    super(manager, sDummyBasicDDFManager);
    if (rows == null) throw new DDFException("Non-null rows Object is required to instantiate a new BasicDDF");
    mUnitType = (typeSpecs != null && typeSpecs.length > 0) ? typeSpecs[1] : null;
    this.initialize(manager, rows, typeSpecs, engineName, namespace, name,
            schema);
  }

  /**
   * Signature without List, useful for creating a dummy DDF used by DDFManager
   *
   * @param manager
   * @throws DDFException
   */
  public BasicDDF(DDFManager manager) throws DDFException {
    super(manager, sDummyBasicDDFManager);
  }

  @Override
  public DDF copy()throws DDFException {
    return null;
  }
  /**
   * For serdes only
   *
   * @throws DDFException
   */
  protected BasicDDF() throws DDFException {
    super(sDummyBasicDDFManager);
  }



  @SuppressWarnings("unchecked")
  public <T> List<T> getList(Class<T> rowType) throws DDFException {
    return (List<T>) this.getRepresentationHandler().get(List.class, rowType);
  }

  public void setList(List<?> data, Class<?> rowType) {
    this.getRepresentationHandler().set(data, List.class, rowType);
  }

  /**
   * Override to snapshot our List<?> into a local variable for serialization
   */
  @Override
  public void beforeSerialization() throws DDFException {
    super.beforeSerialization();
    mData = this.getList(mUnitType);
    mUnitTypeName = (mUnitType != null ? mUnitType.getName() : null);
  }

  /**
   * Override to remove our List<?> snapshot from a local variable for serialization
   */
  @Override
  public void afterSerialization() throws DDFException {
    mData = null;
    mUnitType = null;
    super.afterSerialization();
  }


  /**
   * Special case: if we hold a single object of type IPersistible, then some magic happens: we will return *that*
   * object as a result of the deserialization, instead of this DDF itself. This makes it possible for clients to do
   * things like<br/>
   * <code>
   * PersistenceUri uri = model.persist();
   * Model model = (Model) ddfManager.load(uri);
   * </code> instead of having to do this:<br/>
   * <code>
   * PersistenceUri uri = model.persist();
   * BasicDDF ddf = (BasicDDF) ddfManager.load(uri);
   * Model model = (Model) ddf.getList().get(0);
   * </code>
   */
  @Override
  public ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData)
      throws DDFException {

    try {
      if (!Strings.isNullOrEmpty(mUnitTypeName)) {
        mUnitType = Class.forName(mUnitTypeName);
      }

      if (mData != null) {
        this.setList(mData, mUnitType);

        // See if we need to "unwrap" this object and return the wrapped object instead
        JsonElement deserializedWrappedObject = (serializationData instanceof JsonObject ? //
            ((JsonObject) serializationData).get("mData")
            : null);

        deserializedObject = BasicPersistible.unwrapDeserializedObject(mData, deserializedObject,
            (JsonElement) deserializedWrappedObject);
      }

      return super.afterDeserialization(deserializedObject, serializationData);

    } catch (Exception e) {
      if (e instanceof DDFException) throw (DDFException) e;
      else throw new DDFException(e);
    }
  }
}
