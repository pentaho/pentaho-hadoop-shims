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


package org.pentaho.big.data.impl.shim.pig;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.hadoop.shim.api.pig.PigResult;

/**
 * Created by bryan on 7/9/15.
 */
public class PigResultImpl implements PigResult {
  private final FileObject logFile;
  private final int[] result;
  private final Exception exception;

  public PigResultImpl( FileObject logFile, int[] result, Exception exception ) {
    this.logFile = logFile;
    this.result = result;
    this.exception = exception;
  }

  @Override public FileObject getLogFile() {
    return logFile;
  }

  @Override public int[] getResult() {
    return result;
  }

  @Override public Exception getException() {
    return exception;
  }
}
