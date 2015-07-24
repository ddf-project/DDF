package io.ddf.datasource;

/**
 * author: daoduchuan
 */

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;

import io.ddf.exception.DDFException;


public class S3DataSourceDescriptor extends DataSourceDescriptor {
  private S3DataSourceURI uri;
  private S3DataSourceCredentials credentials;
  private DataSourceSchema schema;
  private FileFormat fileFormat;

  public S3DataSourceDescriptor(S3DataSourceURI uri, S3DataSourceCredentials credentials,
                                DataSourceSchema schema, FileFormat fileFormat){
    super(uri, credentials, schema, fileFormat);
    uri.setAwsKeyID(credentials.getAwsKeyID());
    uri.setAwsSecretKey(credentials.getAwsScretKey());
  }


  @Override
  public DDF load(DDFManager manager) {
    return null;
  }

  public static String parseDelimiter(String serdes) throws DDFException {
    String delimiterPattern = "'separatorchar'\\s*=\\s*'.*'";
    Pattern pattern = Pattern.compile(delimiterPattern);
    Matcher matcher = pattern.matcher(serdes.toLowerCase());
    if (matcher.matches()) {
      // TODO
      String matchedString = matcher.group();
      String str = matchedString.split("\\s*=\\s*")[1];
      if (str.charAt(0) == '\'') {
        str = str.substring(1);
      }
      if (str.charAt(str.length()) == '\'') {
        str = str.substring(0, str.length() - 1);
      }
      return str;
    } else {
      throw  new DDFException("Failed to parse serdes string " + serdes);
    }
  }

  public static String parseQuote(String serdes) throws DDFException {
    Pattern quotePattern = Pattern.compile("'quotechar'\\s*=\\s*'\\W*'");
    Matcher matcher = quotePattern.matcher(serdes.toLowerCase());
    if (matcher.matches()) {
      // TODO
      String matchedString = matcher.group();
      String str = matchedString.split("\\s*=\\s*")[1];
      if (str.charAt(0) == '\'') {
        str = str.substring(1);
      }
      if (str.charAt(str.length()) == '\'') {
        str = str.substring(0, str.length() - 1);
      }
      return str;
    } else {
      throw  new DDFException("Failed to parse serdes string " + serdes);
    }
  }

  public static List<String> parseQuoteAndDelim(String serdes) throws DDFException {
    Pattern propertiesPattern = Pattern.compile("(?<=serdeproperties)\\s*[(]\\s*.*\\s*(?=\\).*)");
    Matcher matcher = propertiesPattern.matcher(serdes.toLowerCase().replace("\n", " "));
    String properties;
    // TODO index?
    if (matcher.matches()) {
      properties = matcher.group();
    } else {
      throw new DDFException("Failed to parse serdes string " + serdes);
    }
    //val properties = propertiesPattern.findFirstIn(serdes.toLowerCase().replace("\n", " ")).get
    String arr = properties;
    String[] lines = arr.split("\n");
    StringBuilder stringBuilder = new StringBuilder();
    if (lines != null) {
      for (int i = 0; i < lines.length; ++i) {
        if (i == 0) {
          stringBuilder = stringBuilder.append(lines[i].charAt(0) == '(' ? lines[i].substring(1) : lines[i]);
        } else {
          stringBuilder = stringBuilder.append("\n")
                  .append(lines[i].charAt(0) == '(' ? lines[i].substring(1) : lines[i]);
        }
      }
    }

    String[] arrs = stringBuilder.toString().split(",(?=(?:[^'']*'[^']*')*[^']*$)");
    String delimStr = arrs[0];
    String quoteStr = arrs[1];
    String quote = parseQuote(quoteStr);
    String delim = parseDelimiter(delimStr);
    // (delim, quote)
    List<String> ret = new ArrayList<String>();
    ret.add(delim);
    ret.add(quote);
    return ret;
  }

  public S3DataSourceDescriptor(String uri, String awsKeyID, String awsSecretKey, String schema, String serdes) throws DDFException {
    this(uri, awsKeyID, awsSecretKey, schema, serdes, DataFormat.CSV);
  }

  public S3DataSourceDescriptor(String uri,
                                String awsKeyID,
                                String awsSecretKey,
                                String schema,
                                String serdes,
                                DataFormat format) throws DDFException {
    List<Schema.Column> columns;
    if (schema != null) {
      columns = new Schema(schema).getColumns();
    } else {
      columns = null;
    }

    TextFileFormat textFileFormat;
    if (serdes != null) {
      List<String> delimandquote = parseQuoteAndDelim(serdes);
      String delim = delimandquote.get(0);
      String quote = delimandquote.get(1);
      // TODO TAG;
      textFileFormat =  new TextFileFormat(format, false, delim, quote);
    } else {
      textFileFormat = new TextFileFormat(format, false, ",", "\"");
    }


    try {
      this.setDataSourceUri(new S3DataSourceURI(uri));
    } catch (URISyntaxException e) {
      throw new DDFException(e);
    }
    this.setDataSourceCredentials(new S3DataSourceCredentials(awsKeyID, awsSecretKey));
    this.setDataSourceSchema(new DataSourceSchema(columns));
    this.setFileFormat(fileFormat);
  }

  public S3DataSourceDescriptor(String uri,
                                String awsKeyID,
                                String awsSecretKey,
                                String schema,
                                DataFormat format,
                                Boolean hasHeader,
                                String delimiter,
                                String quote) throws DDFException {
    List<Schema.Column> columns;
    if (schema != null) {
      columns = new Schema(schema).getColumns();
    } else {
      columns = null;
    }
    TextFileFormat fileFormat = new TextFileFormat(format, hasHeader, delimiter, quote);

    try {
      this.setDataSourceUri(new S3DataSourceURI(uri));
    } catch (URISyntaxException e) {
      throw new DDFException(e);
    }
    this.setDataSourceCredentials(new S3DataSourceCredentials(awsKeyID, awsSecretKey));
    this.setDataSourceSchema(new DataSourceSchema(columns));
    this.setFileFormat(fileFormat);
  }
}
