package io.ddf.datasource;


/**
 */
public class TextFileFormat extends FileFormat {

  private boolean hasheader;

  private String delimiter;

  private String quote;

  public TextFileFormat(DataFormat format, boolean hasHeader, String delimiter, String quote) {
    super(format);
    this.hasheader = hasHeader;
    this.delimiter = delimiter;
    this.quote = quote;
  }

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

  public boolean getHasHeader() {
    return this.hasheader;
  }

  public void setHasHeader(boolean hasheader) {
    this.hasheader = hasheader;
  }
}
