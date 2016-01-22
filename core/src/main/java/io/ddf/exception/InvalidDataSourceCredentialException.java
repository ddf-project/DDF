package io.ddf.exception;

public class InvalidDataSourceCredentialException extends DDFException {

  public InvalidDataSourceCredentialException() {
    this("Invalid Data Source Credential");
  }

  public InvalidDataSourceCredentialException(String msg) {
    super(msg);
  }

  public InvalidDataSourceCredentialException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public InvalidDataSourceCredentialException(Throwable cause) {
    super(cause);
  }
}
