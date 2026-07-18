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



package org.pentaho.hadoop.shim.api.internal.oozie;

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
