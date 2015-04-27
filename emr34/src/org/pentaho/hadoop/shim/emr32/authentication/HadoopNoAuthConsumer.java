/*******************************************************************************
*
* Pentaho Big Data
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

package org.pentaho.hadoop.shim.emr32.authentication;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.emr32.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.emr32.authorization.NoOpHadoopAuthorizationService;

import java.util.Properties;

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
    return new NoOpHadoopAuthorizationService();
  }
}
