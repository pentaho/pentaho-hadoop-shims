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

package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.io.compress.SnappyCodec;

public class SnappyShimImpl extends CommonSnappyShim {
  /**
   * Tests whether hadoop-snappy (not to be confused with other java-based snappy implementations such as jsnappy or
   * snappy-java) plus the native snappy libraries are available.
   *
   * @return true if hadoop-snappy is available on the classpath
   */
  public boolean isHadoopSnappyAvailable() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return SnappyCodec.isNativeCodeLoaded();
    } catch ( Throwable t ) {
      return false;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }
}

