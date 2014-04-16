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

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.kerberos.KerberosUtil;

public class KerberosConsumerUtil {
  private final KerberosUtil kerberosUtil;

  public KerberosConsumerUtil( ) {
    this.kerberosUtil = new KerberosUtil();
  }

  public LoginContext createLoginContext( KerberosAuthenticationProvider authenticationProvider ) throws LoginException {
    final LoginContext loginContext;
    if ( Const.isEmpty( authenticationProvider.getPassword() ) ) {
      if ( !Const.isEmpty( authenticationProvider.getKeytabLocation() ) ) {
        loginContext =
            kerberosUtil.getLoginContextFromKeytab( authenticationProvider.getPrincipal(), authenticationProvider
                .getKeytabLocation() );
      } else {
        loginContext = kerberosUtil.getLoginContextFromKerberosCache( authenticationProvider.getPrincipal() );
      }
    } else {
      loginContext =
          kerberosUtil.getLoginContextFromUsernamePassword( authenticationProvider.getPrincipal(),
              authenticationProvider.getPassword() );
    }
    loginContext.login();
    return loginContext;
  }
}
