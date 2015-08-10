/**
 *
 */
package io.ddf.content;


import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.types.AGloballyAddressable;
import io.ddf.types.IGloballyAddressable;

/**
 *
 */
public abstract class APersistenceHandler extends ADDFFunctionalGroupHandler implements IHandlePersistence {

  public APersistenceHandler(DDF theDDF) {
    super(theDDF);
    this.mPersitable = true;
  }

  private boolean mPersitable = true;

  public boolean isPersistable() {
    if(this.getDDF().getUUID() == null) {
      return false;
    }
    return this.mPersitable;
  }

  public void setPersistable(boolean persistable) {
    this.mPersitable = persistable;
  }

  /**
   * The URI format should be:
   * <p/>
   * <pre>
   * <engine>://<path>
   * </pre>
   * <p/>
   * e.g.,
   * <p/>
   * <pre>
   * basic:///root/ddf/ddf-runtime/basic-ddf-db/com.example/MyDDF.dat
   * </pre>
   *
   * @param uri
   * @return
   */

  public static class PersistenceUri {
    private String mEngine;
    private String mPath;


    public PersistenceUri(String uri) throws DDFException {
      if (Strings.isNullOrEmpty(uri)) throw new DDFException("uri may not be null or empty");

      String[] parts = uri.split("://");
      if (parts.length == 1) {
        mPath = parts[0];
      } else if (parts.length == 2) {
        mEngine = parts[0];
        mPath = parts[1];
      }
    }

    public PersistenceUri(String engine, String path) throws DDFException {
      mEngine = engine;
      mPath = path;
    }

    public String getEngine() {
      return mEngine;
    }

    protected void setEngine(String engine) {
      mEngine = engine;
    }

    public String getPath() {
      return mPath;
    }

    protected void setPath(String path) {
      mPath = path;
    }

    @Override
    public String toString() {
      return String.format("%s://%s", mEngine, mPath);
    }
  }


  /**
   * Base class for objects that can persist themselves, via the DDF persistence mechanism
   */
  public static abstract class APersistible extends AGloballyAddressable implements IGloballyAddressable, IPersistible {

    private static final long serialVersionUID = -5941712506105779254L;


    /**
     * Each subclass is expected to instantiate a new DDF, put this {@link APersistible} object inside of it, and return
     * that DDF for persistence.
     *
     * @return
     * @throws DDFException
     */
    protected abstract DDF newContainerDDFImpl() throws DDFException;


    // //// IPersistible /////

    private DDF createDDFWrapper() throws DDFException {
      DDF ddf = this.newContainerDDFImpl();

      if (ddf == null) throw new DDFException(String.format("Cannot create new container DDF for %s: %s/%s",
          this.getClass(), this.getNamespace(), this.getName()));

      // Make sure we have a namespace and name
      if (Strings.isNullOrEmpty(this.getNamespace())) this.setNamespace(ddf.getNamespace());
      if (Strings.isNullOrEmpty(this.getName())) this.setName(ddf.getSchemaHandler().newTableName(this));

      // Make sure the DDF's names match ours
      ddf.setNamespace(this.getNamespace());
      ddf.getManager().setDDFName(ddf, this.getName());

      return ddf;
    }

    @Override
    public PersistenceUri persist(boolean doOverwrite) throws DDFException {
      this.beforePersisting();

      PersistenceUri uri = this.createDDFWrapper().persist(doOverwrite);

      this.afterPersisting();

      return uri;
    }

    @Override
    public PersistenceUri persist() throws DDFException {
      return this.persist(true);
    }

    @Override
    public void unpersist() throws DDFException {
      this.beforeUnpersisting();

      this.createDDFWrapper().unpersist();

      this.afterUnpersisting();
    }

    @Override
    public void beforePersisting() {
    }


    @Override
    public void afterPersisting() {
    }


    @Override
    public void beforeUnpersisting() {
    }


    @Override
    public void afterUnpersisting() {
    }



    // //// IGloballyAddressable //////
    @Expose private String mEngineName;
    @Expose private String mNamespace;
    @Expose private String mName;

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
      return "persistible";
    }



    @Override
    public void afterSerialization() throws DDFException {
    }

    @Override
    public void beforeSerialization() throws DDFException {
    }

    @Override
    public ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData)
        throws DDFException {
      return deserializedObject;
    }


  }
}
