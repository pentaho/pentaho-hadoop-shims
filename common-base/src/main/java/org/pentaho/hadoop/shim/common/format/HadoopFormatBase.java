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

package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.hadoop.shim.ShimConfigsLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.fs.FileSystemRegistry;

import java.io.InputStream;
import java.util.function.BiConsumer;

/**
 * Class for some base Input/Output Formats functionality, like classloders switching.
 *
 * @author Alexander Buloichik
 */
public class HadoopFormatBase {

  /**
   * Creates a Hadoop Configuration with proper classloader and FileSystem registry setup.
   * This method ensures that custom FileSystem implementations (like PvfsHadoopBridge) can be
   * loaded by Hadoop when FileSystem.get() is called.
   *
   * @param namedCluster The named cluster configuration (can be null)
   * @param classLoader  The classloader to set on the Configuration
   * @return A configured Configuration object
   */
  protected static Configuration createConfigurationWithClassLoader( NamedCluster namedCluster, ClassLoader classLoader ) {
    Configuration confProxy = new ConfigurationProxy();
    confProxy.addResource( "hive-site.xml" );

    if ( namedCluster != null ) {
      BiConsumer<InputStream, String> consumer = ( is, filename ) -> confProxy.addResource( is, filename );
      ShimConfigsLoader.addConfigsAsResources( namedCluster, consumer );
    }

    FileSystemRegistry.registerDefaults();
    FileSystemRegistry.applyToConfiguration( confProxy );
    // Set the classloader on the Configuration so Hadoop can load PvfsHadoopBridge and other shim classes
    confProxy.setClassLoader( classLoader );

    return confProxy;
  }

  protected <R, E extends Exception> R inClassloader( SupplierWithException<R, E> action ) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      try {
        return action.get();
      } catch ( Exception e ) {
        throw new IllegalStateException( e );
      }
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  protected <E extends Exception> void inClassloader( RunnableWithException<E> action ) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      try {
        action.get();
      } catch ( Exception e ) {
        throw new IllegalStateException( e );
      }
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @FunctionalInterface
  public interface SupplierWithException<T, E extends Exception> {
    T get() throws E;
  }

  @FunctionalInterface
  public interface RunnableWithException<E extends Exception> {
    void get() throws E;
  }
}
