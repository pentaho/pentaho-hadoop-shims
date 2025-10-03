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


package org.pentaho.hadoop.shim.api.internal.fs;

import java.net.URI;

/**
 * An abstraction for {@link org.apache.hadoop.fs.Path}.
 *
 * @author Jordan Ganoff (jganoff@gmail.com)
 */
public interface Path {
  /**
   * Converts this to a URI.
   *
   * @return URI that points to this path
   */
  URI toUri();
}
