package org.pentaho.oozie.shim.api;

public class OozieClientException extends Exception {
  private static final long serialVersionUID = 2603554509709959992L;
  
  private final String errorCode;

  public OozieClientException( Throwable cause, String errorCode ) {
    super( cause );
    this.errorCode = errorCode;
  }
  
  public String getErrorCode() {
    return errorCode;
  }
}
