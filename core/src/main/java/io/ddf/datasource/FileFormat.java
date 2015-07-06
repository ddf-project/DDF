package io.ddf.datasource;


/**
 */

public class FileFormat {

  private DataFormat format;

  public FileFormat(DataFormat format) {
    this.format = format;
  }

  public DataFormat getFormat() {
    return this.format;
  }

  public void setFormat(DataFormat format) {
    this.format = format;
  }
}
