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
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseKerberosUserProvider extends UserProvider {
  private static final Map<String, LoginContext> loginContexts = new HashMap<String, LoginContext>();

  private LoginContext loginContext;

  public static void setLoginContext( String uuid, LoginContext loginContext ) {
    synchronized ( loginContexts ) {
      loginContexts.put( uuid, loginContext );
    }
  }

  @Override
  public User getCurrent() throws IOException {
    return create( UserGroupInformation.getCurrentUser() );
  }

  @Override
  public User create( UserGroupInformation ugi ) {
    return new HBaseKerberosUser( ugi, loginContext );
  }

  @Override
  public void setConf( Configuration conf ) {
    super.setConf( conf );
    String loginContextUuid = conf.get( HBaseKerberosShim.PENTAHO_LOGIN_CONTEXT_UUID );
    if ( loginContextUuid != null ) {
      synchronized ( loginContexts ) {
        loginContext = loginContexts.get( loginContextUuid );
      }
    }
  }
}
