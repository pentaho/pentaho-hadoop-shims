/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdp21.authorization;

import org.junit.Test;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.hdp21.delegating.DelegatingHadoopShim;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 10/7/14 Time: 3:48 PM
 */
public class AuthenticatingHadoopShimTest {
  static AtomicInteger countInstancesPerCLassLoader = new AtomicInteger( 0 );

//  public class FakeActivator {
//    public FakeActivator() {
//      AuthenticatingHadoopShimTest.countInstancesPerCLassLoader.incrementAndGet();
//    }
//  }

  @Test
  public void testOnLoadNoAuthSuccess() throws Exception {
    AuthenticatingHadoopShim shim = new AuthenticatingHadoopShim();
    HadoopConfiguration hc = mock( HadoopConfiguration.class );
    Properties prop = new Properties();
    prop.put( DelegatingHadoopShim.SUPER_USER, NoAuthenticationAuthenticationProvider.NO_AUTH_ID );
    prop.put( "activator.classes", FakeActivator.class.getCanonicalName() );
    when( hc.getConfigProperties() ).thenReturn( prop );
    HadoopConfigurationFileSystemManager fsm = mock( HadoopConfigurationFileSystemManager.class );
    try {
      shim.onLoad( hc, fsm );
    } catch ( RuntimeException e ) {
      //Predictable
    }
    assertEquals( "Was tried to instantiate activator class with no_auth", 0, countInstancesPerCLassLoader.get() );
  }

  @Test
  public void testOnLoadWithActivatorSuccess() throws Exception {
    AuthenticatingHadoopShim shim = new AuthenticatingHadoopShim();
    HadoopConfiguration hc = mock( HadoopConfiguration.class );
    Properties prop = new Properties();
    prop.put( DelegatingHadoopShim.SUPER_USER, "any other no auth" );
    prop.put( "activator.classes", FakeActivator.class.getCanonicalName() );
    when( hc.getConfigProperties() ).thenReturn( prop );
    HadoopConfigurationFileSystemManager fsm = mock( HadoopConfigurationFileSystemManager.class );
    try {
      shim.onLoad( hc, fsm );
    } catch ( RuntimeException e ) {
      //Predictable
    }
    assertEquals( "Was not tried to instantiate activator class with no_auth", 1, countInstancesPerCLassLoader.get() );
  }
}
