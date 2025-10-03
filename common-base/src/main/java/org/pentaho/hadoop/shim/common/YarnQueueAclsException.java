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

package org.pentaho.hadoop.shim.common;

public class YarnQueueAclsException extends RuntimeException {
  public YarnQueueAclsException() {
    super();
  }

  public YarnQueueAclsException( String message ) {
    super( message );
  }

  public YarnQueueAclsException( String message, Throwable cause ) {
    super( message, cause );
  }

  public YarnQueueAclsException( Throwable cause ) {
    super( cause );
  }

  protected YarnQueueAclsException( String message, Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace ) {
    super( message, cause, enableSuppression, writableStackTrace );
  }
}
