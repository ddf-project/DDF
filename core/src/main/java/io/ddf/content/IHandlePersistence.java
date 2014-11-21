package io.ddf.content;


import io.ddf.content.APersistenceHandler.PersistenceUri;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

public interface IHandlePersistence extends IHandleDDFFunctionalGroup {

  interface IPersistible extends ISerializable {
    PersistenceUri persist(boolean doOverwrite) throws DDFException;

    PersistenceUri persist() throws DDFException;

    void unpersist() throws DDFException;

    void beforePersisting() throws DDFException;

    void afterPersisting();

    void beforeUnpersisting();

    void afterUnpersisting();
  }


  /**
   * Returns a list of existing namespaces we have in persistent storage
   *
   * @return
   * @throws DDFException
   */
  List<String> listNamespaces() throws DDFException;

  /**
   * Returns a list of existing objects we have in the given namespace in persistent storage
   *
   * @param namespace
   * @return
   * @throws DDFException
   */
  List<String> listItems(String namespace) throws DDFException;

  /**
   * Saves the current DDF to our default persistent storage, at minimum by namespace and name, together with the DDF's
   * metadata, and in whatever serialized format that can be read back in later.
   *
   * @param doOverwrite overwrites if true
   * @return PersistenceUri of persisted location
   * @throws DDFException if doOverwrite is false and the destination already exists
   */
  PersistenceUri persist(boolean doOverwrite) throws DDFException;

  /**
   * Deletes the identified DDF "file" from persistent storage. This does not affect any DDF currently loaded in memory
   * or in processing space.
   *
   * @param namespace
   * @param name
   * @throws DDFException if specified target does not exist
   */
  void unpersist(String namespace, String name) throws DDFException;

  /**
   * Copies the identified DDF "files" from the specified source to destination.
   *
   * @param fromNamespace
   * @param fromName
   * @param toNamespace
   * @param toName
   * @param doOverwrite   overwrites if true
   * @throws DDFException if doOverwrite is false and the destination already exists
   */
  void duplicate(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException;

  /**
   * Same as duplicate followed by unpersist() of the old copy
   *
   * @param fromNamespace
   * @param fromName
   * @param toNamespace
   * @param toName
   * @param doOverwrite
   * @throws DDFException
   */
  void rename(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException;

  /**
   * Loads from default persistent storage into a DDF
   *
   * @param namespace
   * @param name
   * @return
   * @throws DDFException , e.g., if file does not exist
   */
  IPersistible load(String namespace, String name) throws DDFException;

  /**
   * Loads DDF from given URI. The URI format should be:
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
   * <p/>
   * If the engine is not specified, the current DDF's engine is used.
   *
   * @param uri
   * @return
   * @throws DDFException
   */
  IPersistible load(String uri) throws DDFException;

  IPersistible load(PersistenceUri uri) throws DDFException;
}
