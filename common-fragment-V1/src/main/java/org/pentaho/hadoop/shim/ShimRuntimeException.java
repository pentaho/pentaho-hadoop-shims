package org.pentaho.hadoop.shim;

/**
 * Indicates a runtime error occured while working with a shim
 */
public class ShimRuntimeException extends RuntimeException {

  public ShimRuntimeException( String message, Exception ex ) {
    super( message, ex );
  }

  public ShimRuntimeException( Exception ex ) {
    super( ex );
  }
}
