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

package org.pentaho.hadoop.shim.api.pig;

import org.apache.commons.vfs2.FileObject;

/**
 * Created by bryan on 7/9/15.
 */
public interface PigResult {
  FileObject getLogFile();

  int[] getResult();

  Exception getException();
}
