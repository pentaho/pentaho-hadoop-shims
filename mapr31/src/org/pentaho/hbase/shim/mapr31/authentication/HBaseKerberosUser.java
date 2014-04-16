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

package org.pentaho.hbase.shim.mapr31.authentication;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.hadoop.hbase.security.User.SecureHadoopUser;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseKerberosUser extends SecureHadoopUser {
  private final LoginContext loginContext;

  public HBaseKerberosUser( UserGroupInformation ugi, LoginContext loginContext ) {
    super( ugi );
    this.loginContext = loginContext;
  }

  @Override
  public String getName() {
    if ( loginContext == null ) {
      return super.getName();
    }
    List<Principal> principals = new ArrayList<Principal>( loginContext.getSubject().getPrincipals() );
    if ( principals.size() != 1 ) {
      throw new RuntimeException( "Expected 1 principal, found " + principals.size() );
    }
    return principals.get( 0 ).getName();
  }

  @Override
  public String getShortName() {
    if ( loginContext == null ) {
      return super.getShortName();
    }
    try {
      return new HadoopKerberosName( getName() ).getShortName();
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }
  }

  @Override
  public <T> T runAs( PrivilegedAction<T> action ) {
    if ( loginContext == null ) {
      return super.runAs( action );
    }
    return Subject.doAs( loginContext.getSubject(), action );
  }

  @Override
  public <T> T runAs( PrivilegedExceptionAction<T> action ) throws IOException, InterruptedException {
    if ( loginContext == null ) {
      return super.runAs( action );
    }
    try {
      return Subject.doAs( loginContext.getSubject(), action );
    } catch ( PrivilegedActionException e ) {
      throw new IOException( e );
    }
  }
}
