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



package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.util.NativeCodeLoader;

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
      return  NativeCodeLoader.isNativeCodeLoaded() ;
    } catch ( Throwable t ) {
      return false;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }
}

