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

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.security.User;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.mapr31.authentication.context.KerberosAuthenticationContext;
import org.pentaho.hadoop.shim.mapr31.authentication.context.PrivilegedCallable;
import org.pentaho.hadoop.shim.mapr31.authorization.KerberosInvocationHandler;

public class HiveKerberosConsumer implements AuthenticationConsumer<Driver, KerberosAuthenticationProvider> {
  private final KerberosConsumerUtil kerberosUtil;
  private final Driver delegate;

  public HiveKerberosConsumer( Driver delegate ) {
    this.kerberosUtil = new KerberosConsumerUtil();
    this.delegate = delegate;
  }

  @Override
  public Driver consume( final KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    KerberosAuthenticationContext kerberosAuthenticationContext =
        new KerberosAuthenticationContext( new PrivilegedCallable<LoginContext>() {

          @Override
          public LoginContext call() throws PrivilegedActionException {
            LoginContext result;
            try {
              result = kerberosUtil.createLoginContext( authenticationProvider );
            } catch ( LoginException e ) {
              throw new PrivilegedActionException( e );
            }
            Principal kerbPrincipal = new ArrayList<Principal>( result.getSubject().getPrincipals() ).get( 0 );
            result.getSubject().getPrincipals().add( new User( kerbPrincipal.getName() ) );
            return result;
          }
        } );
    return KerberosInvocationHandler.forObject( kerberosAuthenticationContext, delegate, new HashSet<Class<?>>( Arrays
        .<Class<?>> asList( Driver.class, Connection.class, DatabaseMetaData.class, ResultSetMetaData.class,
            ResultSet.class ) ) );
  }
}
