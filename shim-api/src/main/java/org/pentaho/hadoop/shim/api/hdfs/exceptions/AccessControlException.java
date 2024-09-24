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

package org.pentaho.hadoop.shim.api.hdfs.exceptions;

import java.io.IOException;

/**
 * Created by bryan on 8/19/15.
 */
public class AccessControlException extends IOException {
  public AccessControlException( String message, Throwable cause ) {
    super( message, cause );
  }
}
