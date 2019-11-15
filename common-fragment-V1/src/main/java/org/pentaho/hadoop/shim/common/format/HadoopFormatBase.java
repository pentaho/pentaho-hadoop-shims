/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.hadoop.shim.common.format;

/**
 * Class for some base Input/Output Formats functionality, like classloders switching.
 *
 * @author Alexander Buloichik
 */
public class HadoopFormatBase {

  protected <R> R inClassloader( SupplierWithException<R> action ) {
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

  protected void inClassloader( RunnableWithException action ) {
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

  // we should rethink this design.  I believe this was a
  // convenience to allow actions in lambdas that may thrown checked
  // exceptions.
  @FunctionalInterface
  public interface SupplierWithException<T> {
    T get() throws Exception;
  }

  @FunctionalInterface
  public interface RunnableWithException {
    void get() throws Exception;
  }
}
