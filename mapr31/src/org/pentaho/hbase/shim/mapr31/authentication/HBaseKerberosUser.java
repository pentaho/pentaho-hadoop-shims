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

import org.apache.hadoop.hbase.security.User.SecureHadoopUser;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.pentaho.hadoop.shim.mapr31.authentication.context.KerberosAuthenticationContext;

public class HBaseKerberosUser extends SecureHadoopUser {
  private final KerberosAuthenticationContext kerberosAuthenticationContext;

  public HBaseKerberosUser( UserGroupInformation ugi, KerberosAuthenticationContext kerberosAuthenticationContext ) {
    super( ugi );
    this.kerberosAuthenticationContext = kerberosAuthenticationContext;
  }

  @Override
  public String getName() {
    if ( kerberosAuthenticationContext == null ) {
      return super.getName();
    }
    List<Principal> principals;
    try {
      principals =
          new ArrayList<Principal>( kerberosAuthenticationContext.getLoginContext().getSubject().getPrincipals() );
    } catch ( PrivilegedActionException e ) {
      throw new RuntimeException( e );
    }
    if ( principals.size() != 1 ) {
      throw new RuntimeException( "Expected 1 principal, found " + principals.size() );
    }
    return principals.get( 0 ).getName();
  }

  @Override
  public String getShortName() {
    if ( kerberosAuthenticationContext == null ) {
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
    if ( kerberosAuthenticationContext == null ) {
      return super.runAs( action );
    }
    return kerberosAuthenticationContext.doAs( action );
  }

  @Override
  public <T> T runAs( PrivilegedExceptionAction<T> action ) throws IOException, InterruptedException {
    if ( kerberosAuthenticationContext == null ) {
      return super.runAs( action );
    }
    try {
      return kerberosAuthenticationContext.doAs( action );
    } catch ( PrivilegedActionException e ) {
      throw new IOException( e );
    }
  }

  @Override
  public String toString() {
    return getName();
  }
}
