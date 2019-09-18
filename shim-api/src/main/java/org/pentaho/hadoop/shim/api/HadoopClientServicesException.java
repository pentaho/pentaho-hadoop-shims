package org.pentaho.hadoop.shim.api;

public class HadoopClientServicesException extends Exception {
  private final String errorCode;

  public HadoopClientServicesException( Throwable cause ) {
    this( cause, null );
  }

  public HadoopClientServicesException( Throwable cause, String errorCode ) {
    super( cause );
    this.errorCode = errorCode;
  }

  public String getErrorCode() {
    return this.errorCode;
  }

}
