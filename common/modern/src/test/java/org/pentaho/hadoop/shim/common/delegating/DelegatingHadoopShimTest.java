/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.shim.common.delegating;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.common.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * User: Dzmitry Stsiapanau Date: 02/10/2016 Time: 13:11
 */

public class DelegatingHadoopShimTest {
  private DelegatingHadoopShim dhs;
  private HadoopShim hadoopShimMock;
  private HadoopAuthorizationService has;

  @org.junit.Before
  public void setUp() throws Exception {
    dhs = new DelegatingHadoopShim();
    has = mock( HadoopAuthorizationService.class );
    hadoopShimMock = mock( HadoopShim.class );
    when( has.getShim( HadoopShim.class ) ).thenReturn( hadoopShimMock );
    dhs.setHadoopAuthorizationService( has );
  }

  @org.junit.Test
  public void testOnLoad() throws Exception {
    HadoopConfiguration hc = mock( HadoopConfiguration.class );
    HadoopConfigurationFileSystemManager fsm = mock( HadoopConfigurationFileSystemManager.class );
    dhs.onLoad( hc, fsm );
    verify( hadoopShimMock ).onLoad( hc, fsm );
  }

  @org.junit.Test
  public void testSetHadoopAuthorizationService() throws Exception {
    Field delegate = dhs.getClass().getDeclaredField( "delegate" );
    delegate.setAccessible( true );
    HadoopShim hs = (HadoopShim) delegate.get( dhs );
    assertEquals( hs, hadoopShimMock );
  }

  @org.junit.Test
  public void testGetVersion() throws Exception {
    dhs.getVersion();
    verify( hadoopShimMock ).getVersion();
  }

  @org.junit.Test
  public void testGetHiveJdbcDriver() throws Exception {
    dhs.getHiveJdbcDriver();
    verify( hadoopShimMock ).getHiveJdbcDriver();
  }

  @org.junit.Test
  public void testGetJdbcDriver() throws Exception {
    dhs.getJdbcDriver( anyString() );
    verify( hadoopShimMock ).getJdbcDriver( anyString() );
  }

  @org.junit.Test
  public void testGetNamenodeConnectionInfo() throws Exception {
    Configuration c = mock( Configuration.class );
    dhs.getNamenodeConnectionInfo( c );
    verify( hadoopShimMock ).getNamenodeConnectionInfo( c );
  }

  @org.junit.Test
  public void testGetJobtrackerConnectionInfo() throws Exception {
    Configuration c = mock( Configuration.class );
    dhs.getJobtrackerConnectionInfo( c );
    verify( hadoopShimMock ).getJobtrackerConnectionInfo( c );
  }

  @org.junit.Test
  public void testGetHadoopVersion() throws Exception {
    dhs.getHadoopVersion();
    verify( hadoopShimMock ).getHadoopVersion();
  }

  @org.junit.Test
  public void testCreateConfiguration() throws Exception {
    dhs.createConfiguration();
    verify( hadoopShimMock ).createConfiguration();
  }

  @org.junit.Test
  public void testGetFileSystem() throws Exception {
    Configuration c = mock( Configuration.class );

    dhs.getFileSystem( c );
    verify( hadoopShimMock ).getFileSystem( c );
  }

  @org.junit.Test
  public void testConfigureConnectionInformation() throws Exception {
    Configuration c = mock( Configuration.class );
    dhs.configureConnectionInformation( "", "", "", "", c, Collections.<String>emptyList() );
    verify( hadoopShimMock ).configureConnectionInformation( "", "", "", "", c, Collections.<String>emptyList() );
  }

  @org.junit.Test
  public void testGetDistributedCacheUtil() throws Exception {
    dhs.getDistributedCacheUtil();
    verify( hadoopShimMock ).getDistributedCacheUtil();
  }

  @org.junit.Test
  public void testSubmitJob() throws Exception {
    Configuration c = mock( Configuration.class );
    dhs.submitJob( c );
    verify( hadoopShimMock ).submitJob( c );
  }

  @org.junit.Test
  public void testGetHadoopWritableCompatibleClass() throws Exception {
    ValueMetaInterface vai = mock( ValueMetaInterface.class );
    dhs.getHadoopWritableCompatibleClass( vai );
    verify( hadoopShimMock ).getHadoopWritableCompatibleClass( vai );
  }

  @org.junit.Test
  public void testGetPentahoMapReduceCombinerClass() throws Exception {
    dhs.getPentahoMapReduceCombinerClass();
    verify( hadoopShimMock ).getPentahoMapReduceCombinerClass();
  }

  @org.junit.Test
  public void testGetPentahoMapReduceReducerClass() throws Exception {
    dhs.getPentahoMapReduceReducerClass();
    verify( hadoopShimMock ).getPentahoMapReduceReducerClass();
  }

  @org.junit.Test
  public void testGetPentahoMapReduceMapRunnerClass() throws Exception {
    dhs.getPentahoMapReduceMapRunnerClass();
    verify( hadoopShimMock ).getPentahoMapReduceMapRunnerClass();
  }

  @org.junit.Test
  public void testCheckAllMethodsDelegated() throws Exception {
    final AtomicBoolean called = new AtomicBoolean( false );
    has = mock( HadoopAuthorizationService.class );
    hadoopShimMock = mock( HadoopShim.class, new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        called.set( true );
        return null;
      }
    } );
    when( has.getShim( HadoopShim.class ) ).thenReturn( hadoopShimMock );
    dhs.setHadoopAuthorizationService( has );
    Method[] methods = HadoopShim.class.getDeclaredMethods();
    System.out.println( "Methods:" + Arrays.toString( methods ) );
    for ( Method m : methods ) {
      if ( !m.isAccessible() ) {
        continue;
      }
      called.set( false );
      m.invoke( dhs, DelegatingUtils.createArgs( m.getParameterTypes() ) );
      assertTrue( "Method was not delegated " + m.getName(), called.get() );
    }
  }
}
