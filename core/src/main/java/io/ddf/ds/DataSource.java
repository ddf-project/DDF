package io.ddf.ds;


import io.ddf.datasource.FileFormat;
import io.ddf.exception.DDFException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 */
public class DataSource {

  private UUID id;

  private URI uri;

  private List<DSUserCredentials> dsUserCredentialsList = new ArrayList<DSUserCredentials>();

  private List<DataSet> dataSetList = new ArrayList<DataSet>();

  public DataSource(UUID id, URI uri, List<DSUserCredentials> dsUserCredentialsList, List<DataSet> dataSetList) {
    this.id = id;
    this.uri = uri;
    this.dsUserCredentialsList = dsUserCredentialsList;
    this.dataSetList = dataSetList;
  }

  public DataSource(UUID id, URI uri) {
    this(id, uri, new ArrayList<DSUserCredentials>(), new ArrayList<DataSet>());
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public URI getUri() {
    return uri;
  }

  public void setUri(URI uri) {
    this.uri = uri;
  }

  public void addDsUserCredentials(DSUserCredentials dsUserCredentials) throws DDFException {
    if(dsUserCredentials != null) {
      this.dsUserCredentialsList.add(dsUserCredentials);
    } else {
      throw new DDFException("dsUserCredentials is null");
    }
  }

  public List<DSUserCredentials> getDsUserCredentialsList() {
    return new ArrayList<DSUserCredentials>(dsUserCredentialsList);
  }

  public void removeDsUserCredentials(UUID id) {
    for(DSUserCredentials cred: dsUserCredentialsList) {
      if(cred.getId() == id) {
        dsUserCredentialsList.remove(cred);
      }
    }
  }

  public List<DataSet> getDataSetList() {
    return new ArrayList<DataSet>(dataSetList);
  }

  public void addDataSet(DataSet dataSet) throws DDFException {
    if(dataSet != null) {
      this.dataSetList.add(dataSet);
    } else {
      throw new DDFException("dataSet is null");
    }
  }

  public void removeDataset(UUID id) {
    for(DataSet dataset: dataSetList) {
      if(dataset.getId() == id) {
        dataSetList.remove(dataset);
      }
    }
  }
}
