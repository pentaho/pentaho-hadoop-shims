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
package org.pentaho.hadoop.shim.common.format;

/**
 * Class for some base Input/Output Formats functionality, like classloders switching.
 *
 * @author Alexander Buloichik
 */
public class HadoopFormatBase {

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
