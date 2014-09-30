package io.ddf.exception;


/**
 */
public class DDFException extends Exception {

  private static final long serialVersionUID = 8871762342909779405L;


  public DDFException(String msg) {
    super(msg);
  }

  public DDFException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public DDFException(Throwable cause) {
    super(cause);
  }
}
