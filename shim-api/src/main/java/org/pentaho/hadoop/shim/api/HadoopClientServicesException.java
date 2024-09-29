/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
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
