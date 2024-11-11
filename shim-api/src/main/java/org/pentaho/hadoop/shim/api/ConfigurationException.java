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


package org.pentaho.hadoop.shim.api;

public class ConfigurationException extends Exception {
  private static final long serialVersionUID = 1L;

  public ConfigurationException( String message ) {
    super( message );
  }

  public ConfigurationException( String message, Throwable cause ) {
    super( message, cause );
  }
}
