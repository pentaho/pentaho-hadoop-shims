/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

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
