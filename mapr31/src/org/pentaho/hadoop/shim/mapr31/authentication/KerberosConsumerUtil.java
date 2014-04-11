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
