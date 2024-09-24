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

package org.pentaho.hadoop.shim.spi;

import org.pentaho.hadoop.shim.ShimVersion;

/**
 * Represents a type of Hadoop shim. Shims provide an abstraction over a set of APIs that depend upon a set of specific
 * Hadoop libraries. Their implementations must be abstracted so that they may be swapped out at runtime.
 */
public interface PentahoHadoopShim {
  /**
   * @return the version of this shim
   */
  ShimVersion getVersion();
}
