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

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;

public class KerberosAuthenticationContext implements AuthenticationContext {
  private static final int FIVE_MINUTES = 1000 * 60 * 5;
  private final PrivilegedCallable<LoginContext> loginContextAction;
  private LoginContext loginContext;
  private long endTime = 0;

  public KerberosAuthenticationContext( PrivilegedCallable<LoginContext> loginContextAction ) {
    this.loginContextAction = loginContextAction;
  }

  private synchronized void initIfNecessary() throws PrivilegedActionException {
    if ( System.currentTimeMillis() + FIVE_MINUTES > endTime ) {
      loginContext = loginContextAction.call();
      endTime =
          ( (KerberosTicket) new ArrayList<Object>( loginContext.getSubject().getPrivateCredentials() ).get( 0 ) )
              .getEndTime().getTime();
    }
  }

  @Override
  public <T> T doAs( PrivilegedExceptionAction<T> privilegedExceptionAction ) throws PrivilegedActionException {
    initIfNecessary();
    return Subject.doAs( loginContext.getSubject(), privilegedExceptionAction );
  }

  public LoginContext getLoginContext() throws PrivilegedActionException {
    initIfNecessary();
    return loginContext;
  }

  @Override
  public <T> T doAs( java.security.PrivilegedAction<T> privilegedAction ) {
    try {
      initIfNecessary();
    } catch ( PrivilegedActionException e ) {
      throw new RuntimeException( e.getCause() );
    }
    return Subject.doAs( loginContext.getSubject(), privilegedAction );
  }
}
