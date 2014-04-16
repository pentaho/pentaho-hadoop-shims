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

package org.pentaho.hadoop.shim.mapr31.authentication;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.UserSpoofingHadoopAuthorizationCallable;
import org.pentaho.hadoop.shim.mapr31.authorization.UserSpoofingHadoopAuthorizationService;

import com.mapr.fs.proto.Security.TicketAndKey;
import com.mapr.login.client.MapRLoginHttpsClient;

public class MapRSuperUserKerberosConsumer implements
    AuthenticationConsumer<HadoopAuthorizationService, KerberosAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "MapRSuperUserKerberosConsumer", name = "MapRSuperUserKerberosConsumer" )
  public static class MapRSuperUserKerberosConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "MapRSuperUserKerberosConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return MapRSuperUserKerberosConsumer.class;
    }
  }

  private final KerberosConsumerUtil kerberosUtil;
  private final Properties props;

  public MapRSuperUserKerberosConsumer( Properties props ) {
    this.kerberosUtil = new KerberosConsumerUtil();
    this.props = props;
  }

  @Override
  public HadoopAuthorizationService consume( final KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable;
    try {
      userSpoofingHadoopAuthorizationCallable = new UserSpoofingHadoopAuthorizationCallable() {
        private LoginContext loginContext = kerberosUtil.createLoginContext( authenticationProvider );

        @Override
        public TicketAndKey call() throws AuthenticationConsumptionException {
          try {
            return Subject.doAs( loginContext.getSubject(), new PrivilegedExceptionAction<TicketAndKey>() {

              @Override
              public TicketAndKey run() throws Exception {
                return new MapRLoginHttpsClient().getMapRCredentialsViaKerberos( 1209600000L );
              }
            } );
          } catch ( PrivilegedActionException e ) {
            throw new AuthenticationConsumptionException( e );
          }
        }

        @Override
        public LoginContext getLoginContext() {
          return loginContext;
        }

        @Override
        public Properties getConfigProperties() {
          return props;
        }
      };
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    }
    return new UserSpoofingHadoopAuthorizationService( userSpoofingHadoopAuthorizationCallable );
  }
}
