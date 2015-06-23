package io.ddf.datasource;


/**
 */

public class FileFormat {

  private DataFormat format;

  private boolean header;

  private String delimiter;

  private String quote;

  public String getDelimiter() {
    return this.delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public String getQuote() {
    return this.quote;
  }

  public void setQuote(String quote) {
    this.quote = quote;
  }

  public boolean getHeader() {
    return this.header;
  }

  public void setHeader(boolean header) {
    this.header = header;
  }

  public DataFormat getFormat() {
    return this.format;
  }

  public void setFormat(DataFormat format) {
    this.format = format;
  }
}
