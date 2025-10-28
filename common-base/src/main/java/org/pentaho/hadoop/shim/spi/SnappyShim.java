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


package org.pentaho.hadoop.shim.spi;

import java.io.InputStream;
import java.io.OutputStream;

public interface SnappyShim extends PentahoHadoopShim {
  /**
   * Tests whether hadoop-snappy (not to be confused with other java-based snappy implementations such as jsnappy or
   * snappy-java) plus the native snappy libraries are available.
   *
   * @return true if hadoop-snappy is available on the classpath
   */
  boolean isHadoopSnappyAvailable();

  /**
   * Gets a CompressionInputStream that uses the snappy codec and wraps the supplied base input stream.
   *
   * @param in the base input stream to wrap around
   * @return an InputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  InputStream getSnappyInputStream( InputStream in ) throws Exception;

  /**
   * Gets an InputStream that uses the snappy codec and wraps the supplied base input stream.
   *
   * @param the buffer size for the codec to use (in bytes)
   * @param in  the base input stream to wrap around
   * @return an InputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  InputStream getSnappyInputStream( int bufferSize, InputStream in ) throws Exception;

  /**
   * Gets an OutputStream that uses the snappy codec and wraps the supplied base output stream.
   *
   * @param the buffer size for the codec to use (in bytes)
   * @param out the base output stream to wrap around
   * @return a OutputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  OutputStream getSnappyOutputStream( OutputStream out ) throws Exception;

  /**
   * Gets an OutputStream that uses the snappy codec and wraps the supplied base output stream.
   *
   * @param the buffer size for the codec to use (in bytes)
   * @param out the base output stream to wrap around
   * @return a OutputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  OutputStream getSnappyOutputStream( int bufferSize, OutputStream out ) throws Exception;
}
