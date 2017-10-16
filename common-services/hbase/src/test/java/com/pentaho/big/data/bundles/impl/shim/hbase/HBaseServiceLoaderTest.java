/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.lang.reflect.InvocationTargetException;

import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 2/2/16.
 */
public class HBaseServiceLoaderTest {
  private BundleContext bundleContext;
  //private HBaseServiceLoader hBaseServiceLoader;
  private HadoopConfiguration hadoopConfiguration;

  @Before
  public void setup() throws ConfigurationException {
    bundleContext = mock( BundleContext.class );
    //hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    hadoopConfiguration = mock( HadoopConfiguration.class );
    //    hBaseServiceLoader =
    //      new HBaseServiceLoader( bundleContext, shimBridgingServiceTracker, hadoopConfigurationBootstrap );
  }

  @Test
  public void testTwoArgConstructor() throws ConfigurationException {
    //assertNotNull( new HBaseServiceLoader( bundleContext, shimBridgingServiceTracker ) );
  }

  @Test
  public void testOnClassLoaderAvailable() {
    ClassLoader classLoader = mock( ClassLoader.class );
    //hBaseServiceLoader.onClassLoaderAvailable( classLoader );
    //verifyNoMoreInteractions( classLoader );
  }

  @Test
  public void testOnConfigurationClose() {
    //    hBaseServiceLoader.onConfigurationClose( hadoopConfiguration );
    //    verify( shimBridgingServiceTracker ).unregister( hadoopConfiguration );
  }


  @Test
  public void testOnConfigurationOpenFailure()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    //    hBaseServiceLoader.onConfigurationOpen( hadoopConfiguration, true );
    //    verify( shimBridgingServiceTracker, never() )
    //      .registerWithClassloader( any(), any( Class.class ), anyString(), any( BundleContext.class ),
    //        any( ClassLoader.class ), any( Class[].class ), any( Object[].class ) );
  }
}
