package org.pentaho.hadoop.shim;

/**
 * Indicates a runtime error occured while working with a shim
 */
public class ShimException extends Exception {

  public ShimException(String message, Exception ex ) {
      super( message, ex );
  }

  public ShimException(InterruptedException ex ) {
    super( ex );
  }

  public ShimException( String message ) {
    super( message );
  }
}
