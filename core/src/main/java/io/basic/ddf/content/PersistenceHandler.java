/**
 * 
 */
package io.basic.ddf.content;


import java.io.IOException;
import java.util.List;
import io.basic.ddf.BasicDDF;
import io.ddf.DDF;
import io.ddf.content.APersistenceHandler;
import io.ddf.content.ISerializable;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.misc.Config;
import io.ddf.types.AGloballyAddressable;
import io.ddf.types.IGloballyAddressable;
import io.ddf.util.Utils;
import io.ddf.util.Utils.JsonSerDes;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * This {@link PersistenceHandler} loads and saves from/to a designated storage area.
 */
public class PersistenceHandler extends APersistenceHandler {

  public PersistenceHandler(DDF theDDF) {
    super(theDDF);
  }


  protected String locateOrCreatePersistenceDirectory() throws DDFException {
    String result, path = null;

    try {
      result = Utils.locateOrCreateDirectory(Config.getBasicPersistenceDir());

    } catch (Exception e) {
      throw new DDFException(String.format("Unable to getPersistenceDirectory(%s)", path), e);
    }

    return result;
  }

  protected String locateOrCreatePersistenceSubdirectory(String subdir) throws DDFException {
    String result = null, path = null;

    try {
      path = String.format("%s/%s", this.locateOrCreatePersistenceDirectory(), subdir);
      result = Utils.locateOrCreateDirectory(path);

    } catch (Exception e) {
      throw new DDFException(String.format("Unable to getPersistenceSubdirectory(%s)", path), e);
    }

    return result;
  }

  protected String getDataFileName() throws DDFException {
    return this.getFilePath(this.getDDF().getNamespace(), this.getDDF().getName(), ".dat");
  }

  protected String getDataFileName(String namespace, String name) throws DDFException {
    return this.getFilePath(namespace, name, ".dat");
  }

  protected String getSchemaFileName() throws DDFException {
    return this.getFilePath(this.getDDF().getNamespace(), this.getDDF().getName(), ".sch");
  }

  protected String getSchemaFileName(String namespace, String name) throws DDFException {
    return this.getFilePath(namespace, name, ".sch");
  }

  private String getFilePath(String namespace, String name, String postfix) throws DDFException {
    String directory = locateOrCreatePersistenceSubdirectory(namespace);
    return String.format("%s/%s%s", directory, name, postfix);
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.ddf.content.IHandlePersistence#save(boolean)
   */
  @Override
  public PersistenceUri persist(boolean doOverwrite) throws DDFException {
    if (this.getDDF() == null) throw new DDFException("DDF cannot be null");

    String dataFile = this.getDataFileName();
    String schemaFile = this.getSchemaFileName();

    try {
      if (!doOverwrite && (Utils.fileExists(dataFile) || Utils.fileExists(schemaFile))) {
        throw new DDFException("DDF already exists in persistence storage, and overwrite option is false");
      }
    } catch (IOException e) {
      throw new DDFException(e);
    }

    try {
      this.getDDF().beforePersisting();

      Utils.writeToFile(dataFile, JsonSerDes.serialize(this.getDDF()) + '\n');
      Utils.writeToFile(schemaFile, JsonSerDes.serialize(this.getDDF().getSchema()) + '\n');

      this.getDDF().afterPersisting();

    } catch (Exception e) {
      if (e instanceof DDFException) throw (DDFException) e;
      else throw new DDFException(e);
    }

    return new PersistenceUri(this.getDDF().getEngine(), dataFile);
  }


  /*
   * (non-Javadoc)
   * 
   * @see io.ddf.content.IHandlePersistence#delete(java.lang.String, java.lang.String)
   */
  @Override
  public void unpersist(String namespace, String name) throws DDFException {
    this.getDDF().beforeUnpersisting();

    Utils.deleteFile(this.getDataFileName(namespace, name));
    Utils.deleteFile(this.getSchemaFileName(namespace, name));

    this.getDDF().afterUnpersisting();
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.ddf.content.IHandlePersistence#copy(java.lang.String, java.lang.String, java.lang.String, java.lang.String,
   * boolean)
   */
  @Override
  public void duplicate(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException {

    IPersistible from = this.load(fromNamespace, fromName);
    if (from instanceof DDF) {
      DDF to = (DDF) from;
      to.setNamespace(toNamespace);
      to.setName(toName);
      to.persist();

    } else {
      throw new DDFException("Can only duplicate DDFs");
    }
  }

  @Override
  public void rename(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException {

    this.duplicate(fromNamespace, fromName, toNamespace, toName, doOverwrite);
    this.unpersist(fromNamespace, fromName);
  }

  @Override
  public IPersistible load(String uri) throws DDFException {
    return (DDF) this.load(new PersistenceUri(uri));
  }

  @Override
  public IPersistible load(PersistenceUri uri) throws DDFException {
    PersistenceUri2 uri2 = new PersistenceUri2(uri);
    return this.load(uri2.getNamespace(), uri2.getName());
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.ddf.content.IHandlePersistence#load(java.lang.String, java.lang.String)
   */
  @Override
  public IPersistible load(String namespace, String name) throws DDFException {
    Object loadedObject, schema = null;


    loadedObject = JsonSerDes.loadFromFile(this.getFilePath(namespace, name, ".dat"));
    if (loadedObject == null) throw new DDFException((String.format("Got null for IPersistible for %s/%s", namespace,
        name)));

    schema = JsonSerDes.loadFromFile(this.getFilePath(namespace, name, ".sch"));
    if (schema == null) throw new DDFException((String.format("Got null for Schema for %s/%s", namespace, name)));


    if (!(loadedObject instanceof IPersistible)) {
      throw new DDFException("Expected object to be IPersistible, got " + loadedObject.getClass());
    }

    if (loadedObject instanceof DDF && schema instanceof Schema) {
      ((DDF) loadedObject).getSchemaHandler().setSchema((Schema) schema);
    }

    return (IPersistible) loadedObject;
  }

  @Override
  public List<String> listNamespaces() throws DDFException {
    return Utils.listSubdirectories(this.locateOrCreatePersistenceDirectory());
  }


  @Override
  public List<String> listItems(String namespace) throws DDFException {
    return Utils.listSubdirectories(this.locateOrCreatePersistenceSubdirectory(namespace));
  }


  /**
   * Like {@link PersistenceUri} but also with namespace and name parsed
   */
  public static class PersistenceUri2 extends PersistenceUri implements IGloballyAddressable {
    public PersistenceUri2(String uri) throws DDFException {
      super(uri);
      this.parsePath();
    }

    public PersistenceUri2(PersistenceUri uri) throws DDFException {
      super(uri.getEngine(), uri.getPath());
      this.parsePath();
    }


    private String mNamespace;
    private String mName;


    /**
     * Parse the path part of the uri into namespace and name
     */
    private void parsePath() {
      if (Strings.isNullOrEmpty(this.getPath())) return;

      String[] parts = this.getPath().split("/");
      if (parts == null || parts.length == 0) return;

      String name = parts[parts.length - 1];
      if (!Strings.isNullOrEmpty(name) && name.toLowerCase().endsWith(".dat") || name.toLowerCase().endsWith(".sch")) {
        name = name.substring(0, name.lastIndexOf('.'));
        // Also trim our current path
        this.setPath(this.getPath().substring(0, this.getPath().lastIndexOf('.')));
      }
      this.setName(name);

      if (parts.length > 1) {
        this.setNamespace(parts[parts.length - 2]);
      }
    }

    /**
     * @return the namespace
     */
    @Override
    public String getNamespace() {
      return mNamespace;
    }

    /**
     * @param namespace
     *          the namespace to set
     */
    @Override
    public void setNamespace(String namespace) {
      this.mNamespace = namespace;
    }

    /**
     * @return the name
     */
    @Override
    public String getName() {
      return mName;
    }

    /**
     * @param name
     *          the name to set
     */
    @Override
    public void setName(String name) {
      this.mName = name;
    }

    @Override
    public String getUri() {
      return AGloballyAddressable.getUri(this);
    }

    @Override
    public String getGlobalObjectType() {
      return "persistence_uri";
    }
  }



  /**
   * Base class for objects that can persist themselves (e.g, Models) via the BasicObjectDDF persistence mechanism
   * 
   */
  public static class BasicPersistible extends APersistible {

    private static final long serialVersionUID = 5827603466305690244L;


    @Override
    protected DDF newContainerDDFImpl() throws DDFException {
      List<Object[]> list = Lists.newArrayList();
      list.add(new Object[] { this, this.getClass().getName() });
      Schema schema = new Schema(this.getName(), "object BLOB, objectClass STRING");

      BasicDDF ddf = new BasicDDF(list, Object[].class, this.getNamespace(), this.getName(), schema);

      return ddf;
    }


    /**
     * Special case: if we hold a single object of type IPersistible, then some magic happens: we will return *that*
     * object as a result of the deserialization, instead of this DDF itself. This makes it possible for clients to do
     * things like<br/>
     * <code>
     *   PersistenceUri uri = model.persist();
     *   Model model = (Model) ddfManager.load(uri);
     * </code> instead of having to do this:<br/>
     * <code>
     *   PersistenceUri uri = model.persist();
     *   BasicDDF ddf = (BasicDDF) ddfManager.load(uri);
     *   Model model = (Model) ddf.getList().get(0);
     * </code>
     * 
     * @throws DDFException
     */
    public static ISerializable unwrapDeserializedObject(//
        List<?> dataRows, ISerializable deserializedObject, JsonElement deserializedWrappedObject) throws DDFException {

      if (dataRows.size() == 1 && deserializedWrappedObject instanceof JsonArray) {
        JsonElement data = ((JsonArray) deserializedWrappedObject).get(0);

        if (data instanceof JsonArray) {
          JsonArray array = (JsonArray) data;
          if (array.size() == 2) {
            // Now we know it's very likely a two-column schema (object BLOB, objectClass STRING)
            JsonElement object = ((JsonArray) data).get(0);
            JsonElement objectClass = ((JsonArray) data).get(1);

            if (objectClass instanceof JsonPrimitive) {
              try {
                Object embeddedObject = new Gson()
                    .fromJson(object.toString(), Class.forName(objectClass.getAsString()));

                if (embeddedObject instanceof ISerializable) {
                  // Yep, it's an ISerializable that we need to unwrap
                  deserializedObject = (ISerializable) embeddedObject;
                }

              } catch (Exception e) {
                if (e instanceof DDFException) throw (DDFException) e;
                else throw new DDFException(String.format("Unable to unwrap object from %s", object.toString()), e);
              }
            }
          }
        }
      }
      return deserializedObject;
    }

  }
}
