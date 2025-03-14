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

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.spi.SnappyShim;

/**
 * Helper class for determining (via reflection) whether various Hadoop compression codecs (such as Snappy) are
 * available on the classpath and for returning Input/Output streams for reading/writing.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: 16312 $
 */
public abstract class CommonSnappyShim implements SnappyShim {

  public static final String IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY = "io.compression.codec.snappy.buffersize";
  public static final int IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT = 256 * 1024;

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  /**
   * Gets a CompressionInputStream that uses the snappy codec and wraps the supplied base input stream.
   *
   * @param in the base input stream to wrap around
   * @return an InputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  public InputStream getSnappyInputStream( InputStream in ) throws Exception {
    return getSnappyInputStream( IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT, in );
  }

  /**
   * Gets an InputStream that uses the snappy codec and wraps the supplied base input stream.
   *
   * @param the buffer size for the codec to use (in bytes)
   * @param in  the base input stream to wrap around
   * @return an InputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  public InputStream getSnappyInputStream( int bufferSize, InputStream in ) throws Exception {
    if ( !isHadoopSnappyAvailable() ) {
      throw new Exception( "Hadoop-snappy does not seem to be available" );
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      SnappyCodec c = new SnappyCodec();
      Configuration newConf = new Configuration();
      newConf.set( IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY, "" + bufferSize );
      c.setConf( newConf );
      return c.createInputStream( in );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  /**
   * Gets an OutputStream that uses the snappy codec and wraps the supplied base output stream.
   *
   * @param the buffer size for the codec to use (in bytes)
   * @param out the base output stream to wrap around
   * @return a OutputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  public OutputStream getSnappyOutputStream( OutputStream out ) throws Exception {
    return getSnappyOutputStream( IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT, out );
  }

  /**
   * Gets an OutputStream that uses the snappy codec and wraps the supplied base output stream.
   *
   * @param the buffer size for the codec to use (in bytes)
   * @param out the base output stream to wrap around
   * @return a OutputStream that uses the Snappy codec
   * @throws Exception if snappy is not available or an error occurs during reflection
   */
  public OutputStream getSnappyOutputStream( int bufferSize, OutputStream out ) throws Exception {
    if ( !isHadoopSnappyAvailable() ) {
      throw new Exception( "Hadoop-snappy does not seem to be available" );
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      SnappyCodec c = new SnappyCodec();
      Configuration newConf = new Configuration();
      newConf.set( IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY, "" + bufferSize );
      c.setConf( newConf );
      return c.createOutputStream( out );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }
}
