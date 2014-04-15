package org.pentaho.hadoop.shim.mapr31.authentication;

import java.security.Principal;
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
import org.pentaho.hadoop.shim.mapr31.authorization.KerberosInvocationHandler;

public class HiveKerberosConsumer implements AuthenticationConsumer<Driver, KerberosAuthenticationProvider> {
  private final KerberosConsumerUtil kerberosUtil;
  private final Driver delegate;

  public HiveKerberosConsumer( Driver delegate ) {
    this.kerberosUtil = new KerberosConsumerUtil();
    this.delegate = delegate;
  }

  @Override
  public Driver consume( KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    try {
      LoginContext loginContext = kerberosUtil.createLoginContext( authenticationProvider );
      Principal kerbPrincipal = new ArrayList<Principal>( loginContext.getSubject().getPrincipals() ).get( 0 );
      loginContext.getSubject().getPrincipals().add( new User( kerbPrincipal.getName() ) );
      return KerberosInvocationHandler.forObject( loginContext, delegate, new HashSet<Class<?>>( Arrays
          .<Class<?>> asList( Driver.class, Connection.class, DatabaseMetaData.class, ResultSetMetaData.class,
              ResultSet.class ) ) );
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    }
  }
}
