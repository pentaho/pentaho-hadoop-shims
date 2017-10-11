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
import org.pentaho.hadoop.shim.common.authorization.HadoopAuthorizationService;
import org.pentaho.oozie.shim.api.OozieClientFactory;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 02/18/2016 Time: 13:56
 */

public class DelegatingOozieClientFactoryTest {
  private DelegatingOozieClientFactory dhs;
  private OozieClientFactory shimMock;
  private HadoopAuthorizationService has;

  @org.junit.Test
  public void testCheckAllMethodsDelegated() throws Exception {
    dhs = new DelegatingOozieClientFactory();
    has = mock( HadoopAuthorizationService.class );
    final AtomicBoolean called = new AtomicBoolean( false );
    shimMock = mock( OozieClientFactory.class, new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        called.set( true );
        return null;
      }
    } );
    when( has.getShim( OozieClientFactory.class ) ).thenReturn( shimMock );
    dhs.setHadoopAuthorizationService( has );
    Method[] methods = OozieClientFactory.class.getDeclaredMethods();
    for ( Method m : methods ) {
      called.set( false );
      m.invoke( dhs, DelegatingUtils.createArgs( m.getParameterTypes() ) );
      assertTrue( "Method was not delegated " + m.getName(), called.get() );
    }
  }
}
