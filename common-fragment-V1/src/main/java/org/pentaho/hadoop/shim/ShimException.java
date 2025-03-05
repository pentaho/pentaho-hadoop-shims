/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/
package org.pentaho.hadoop.shim;

/**
 * Indicates a runtime error occured while working with a shim
 */
public class ShimException extends Exception {

  public ShimException( String message, Exception ex ) {
    super( message, ex );
  }

  public ShimException( InterruptedException ex ) {
    super( ex );
  }

  public ShimException( String message ) {
    super( message );
  }
}
