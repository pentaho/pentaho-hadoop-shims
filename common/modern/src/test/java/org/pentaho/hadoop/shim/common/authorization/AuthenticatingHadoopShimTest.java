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
package org.pentaho.hadoop.shim.common.authorization;

import org.junit.Test;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * User: Dzmitry Stsiapanau Date: 02/18/2016 Time: 15:31
 */

public class AuthenticatingHadoopShimTest {

  private static AtomicBoolean initialized1 = new AtomicBoolean( false );
  private static AtomicBoolean initialized2 = new AtomicBoolean( false );

  public static class ActivatorClass1 {
    public ActivatorClass1() {
      initialized1.set( true );
    }
  }

  public static class ActivatorClass2 {
    public ActivatorClass2() {
      initialized2.set( true );
    }
  }

  @Test
  public void testOnLoad() throws Exception {
    AuthenticatingHadoopShim shim = new AuthenticatingHadoopShim();
    HadoopAuthorizationService has = mock( HadoopAuthorizationService.class );
    HadoopShim shimMock = mock( HadoopShim.class );
    when( has.getShim( HadoopShim.class ) ).thenReturn( shimMock );
    shim.setHadoopAuthorizationService( has );
    HadoopConfiguration conf = mock( HadoopConfiguration.class );
    Properties props = new Properties();
    props.setProperty( "activator.classes",
      ActivatorClass1.class.getName() + "," + ActivatorClass2.class.getName() );
    when( conf.getConfigProperties() ).thenReturn( props );
    HadoopConfigurationFileSystemManager hcfsm = mock( HadoopConfigurationFileSystemManager.class );
    shim.onLoad( conf, hcfsm );
    assertTrue( "Activator class was not loaded", initialized1.get() );
    assertTrue( "Second activator class was not loaded", initialized2.get() );
    verify( shimMock ).onLoad( conf, hcfsm );
  }

  @Test
  public void testOnLoadNoAuthInCaseImpersonationDisabled() throws Exception {
    AuthenticatingHadoopShim shim = spy( new AuthenticatingHadoopShim() );
    HadoopAuthorizationService has = mock( HadoopAuthorizationService.class );
    HadoopShim shimMock = mock( HadoopShim.class );
    when( has.getShim( HadoopShim.class ) ).thenReturn( shimMock );
    shim.setHadoopAuthorizationService( has );
    HadoopConfiguration conf = mock( HadoopConfiguration.class );
    Properties props = new Properties();
    props.setProperty( "authentication.superuser.provider",
      "some-kerberos" );
    when( conf.getConfigProperties() ).thenReturn( props );
    HadoopConfigurationFileSystemManager hcfsm = mock( HadoopConfigurationFileSystemManager.class );
    try {
      shim.onLoad( conf, hcfsm );
    } catch ( RuntimeException e ) {
      assertEquals( "Unable to find relevant provider for chosen authentication method (id of some-kerberos",
        e.getMessage() );
    }

    props = new Properties();
    props.setProperty( "authentication.superuser.provider", "some-kerberos" );
    props.setProperty( "pentaho.authentication.default.mapping.impersonation.type", "list" );
    when( conf.getConfigProperties() ).thenReturn( props );
    try {
      shim.onLoad( conf, hcfsm );
    } catch ( RuntimeException e ) {
      assertEquals( "Unable to find relevant provider for chosen authentication method (id of some-kerberos",
        e.getMessage() );
    }

    props = new Properties();
    props.setProperty( "authentication.superuser.provider", "some-kerberos" );
    props.setProperty( "pentaho.authentication.default.mapping.impersonation.type", "disabled" );
    when( conf.getConfigProperties() ).thenReturn( props );
    try {
      shim.onLoad( conf, hcfsm );
    } catch ( RuntimeException e ) {
      fail( e.toString() );
    }
  }
}
