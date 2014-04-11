package org.pentaho.hadoop.shim.mapr31.authentication;

import java.util.Properties;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.mapr31.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.mapr31.authorization.NoOpHadoopAuthorizationService;

public class MapRSuperUserNoAuthConsumer implements
    AuthenticationConsumer<HadoopAuthorizationService, NoAuthenticationAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "MapRSuperUserNoAuthConsumer", name = "MapRSuperUserNoAuthConsumer" )
  public static class MapRSuperUserNoAuthConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "MapRSuperUserNoAuthConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return MapRSuperUserNoAuthConsumer.class;
    }
  }

  public MapRSuperUserNoAuthConsumer( Properties props ) {
    // Noop
  }

  @Override
  public HadoopAuthorizationService consume( NoAuthenticationAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    return new NoOpHadoopAuthorizationService();
  }
}
