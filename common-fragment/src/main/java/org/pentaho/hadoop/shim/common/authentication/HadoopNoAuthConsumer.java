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

package org.pentaho.hadoop.shim.common.authentication;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.common.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.common.authorization.NoOpHadoopAuthorizationService;

import java.util.Iterator;
import java.util.Properties;
import java.util.ServiceLoader;

public class HadoopNoAuthConsumer implements
  AuthenticationConsumer<HadoopAuthorizationService, NoAuthenticationAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "HadoopNoAuthConsumer", name = "HadoopNoAuthConsumer" )
  public static class HadoopNoAuthConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "HadoopNoAuthConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return HadoopNoAuthConsumer.class;
    }
  }

  public HadoopNoAuthConsumer( Properties props ) {
    // Noop
  }

  @Override
  public HadoopAuthorizationService consume( NoAuthenticationAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    final Iterator<NoOpHadoopAuthorizationService> providers =
      ServiceLoader.load( NoOpHadoopAuthorizationService.class, NoOpHadoopAuthorizationService.class.getClassLoader() )
        .iterator();
    if ( providers.hasNext() ) {
      return providers.next();
    }
    return new NoOpHadoopAuthorizationService();
  }
}
