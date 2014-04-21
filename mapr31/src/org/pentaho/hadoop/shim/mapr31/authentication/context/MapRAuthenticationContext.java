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

package org.pentaho.hadoop.shim.mapr31.authentication.context;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import com.mapr.fs.proto.Security.TicketAndKey;
import com.mapr.login.client.MapRLoginHttpsClient;

public class MapRAuthenticationContext implements AuthenticationContext {
  private static final int FIVE_MINUTES = 1000 * 60 * 5;
  private final KerberosAuthenticationContext kerberosAuthenticationContext;
  private TicketAndKey ticketAndKey;

  public MapRAuthenticationContext( KerberosAuthenticationContext kerberosAuthenticationContext ) {
    this.kerberosAuthenticationContext = kerberosAuthenticationContext;
  }
  
  private synchronized void initIfNecessary() throws PrivilegedActionException {
    synchronized ( this ) {
      if ( ticketAndKey == null || System.currentTimeMillis() + FIVE_MINUTES > ticketAndKey.getExpiryTime() * 1000 ) {
        ticketAndKey = kerberosAuthenticationContext.doAs( new PrivilegedExceptionAction<TicketAndKey>() {

          @Override
          public TicketAndKey run() throws Exception {
            return new MapRLoginHttpsClient().getMapRCredentialsViaKerberos( 1209600000L );
          }
        } );
      }
    }
  }

  @Override
  public <T> T doAs( PrivilegedExceptionAction<T> privilegedExceptionAction ) throws PrivilegedActionException {
    initIfNecessary();
    try {
      return privilegedExceptionAction.run();
    } catch ( Exception e ) {
      if ( e instanceof PrivilegedActionException ) {
        throw (PrivilegedActionException) e;
      }
      throw new PrivilegedActionException( e );
    }
  }

  @Override
  public <T> T doAs( PrivilegedAction<T> privilegedAction ) {
    try {
      initIfNecessary();
    } catch ( PrivilegedActionException e ) {
      throw new RuntimeException( e.getCause() );
    }
    return privilegedAction.run();
  }
}
