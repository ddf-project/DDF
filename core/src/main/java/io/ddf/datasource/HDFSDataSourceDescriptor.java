package io.ddf.datasource;
/**
 * author: daoduchuan, namma
 */

import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

import java.net.URISyntaxException;
import java.util.List;

public class HDFSDataSourceDescriptor extends DataSourceDescriptor {
  public HDFSDataSourceDescriptor(DataSourceURI uri, IDataSourceCredentials credentials,
                                  DataSourceSchema schema, FileFormat fileFormat) {
    super(uri, credentials, schema, fileFormat);
  }
  private String comment = null;

  public String getComment() {
    return this.comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public DDF load(DDFManager manager) {
    return null;
  }

  public  HDFSDataSourceDescriptor(String uri,
                                   String schema,
                                   String serdes,
                                   String originalSource,
                                   DataFormat format) throws DDFException, URISyntaxException {
    List<Schema.Column> columnList;
    if (!Strings.isNullOrEmpty(schema)) {
      columnList = new Schema(schema).getColumns();
    } else {
      columnList = null;
    }
    List<String> delimandquote = S3DataSourceDescriptor.parseQuoteAndDelim(serdes);
    TextFileFormat fileFormat = new TextFileFormat(format, false, delimandquote.get(0), delimandquote.get(1));

    this.setDataSourceUri(new DataSourceURI(uri));
    this.setDataSourceCredentials(null);
    this.setDataSourceSchema(new DataSourceSchema(columnList));
    this.setFileFormat(fileFormat);
    this.setComment(originalSource);
  }

  public HDFSDataSourceDescriptor(String uri,
                                  String schema,
                                  DataFormat format,
                                  Boolean hasHeader,
                                  String delimiter,
                                  String quote,
                                  String comment) throws URISyntaxException {
    TextFileFormat textFileFormat = new TextFileFormat(format, hasHeader, delimiter, quote);
    List<Schema.Column> columnList;
    if (!Strings.isNullOrEmpty(schema)) {
      columnList = new Schema(schema).getColumns();
    } else {
      columnList = null;
    }

    this.setDataSourceUri(new DataSourceURI(uri));
    this.setFileFormat(textFileFormat);
    this.setDataSourceSchema(new DataSourceSchema(columnList));
    this.setFileFormat(textFileFormat);
    this.setComment(comment);

  }
}
