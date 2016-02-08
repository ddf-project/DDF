package io.ddf.exception;

public class UnauthenticatedDataSourceException extends DDFException {

  public UnauthenticatedDataSourceException() {
    this("Unauthenticated Data Source");
  }

  public UnauthenticatedDataSourceException(String msg) {
    super(msg);
  }

  public UnauthenticatedDataSourceException(Throwable cause) {
    super(cause);
  }

  public UnauthenticatedDataSourceException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
