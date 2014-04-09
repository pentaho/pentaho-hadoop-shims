package org.pentaho.hadoop.shim.mapr31.authorization;

import java.net.URLClassLoader;

import org.pentaho.di.core.auth.AuthenticationConsumerPluginType;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.mapr31.authentication.PropertyAuthenticationProviderParser;
import org.pentaho.hadoop.shim.mapr31.authentication.MapRSuperUserKerberosConsumer.MapRSuperUserKerberosConsumerType;
import org.pentaho.hadoop.shim.mapr31.authentication.MapRSuperUserNoAuthConsumer.MapRSuperUserNoAuthConsumerType;
import org.pentaho.hadoop.shim.mapr31.delegatingShims.DelegatingHadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

public class AuthenticatingHadoopShim extends DelegatingHadoopShim {
  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    AuthenticationConsumerPluginType.getInstance().registerPlugin( (URLClassLoader) getClass().getClassLoader(),
        MapRSuperUserKerberosConsumerType.class );
    AuthenticationConsumerPluginType.getInstance().registerPlugin( (URLClassLoader) getClass().getClassLoader(),
        MapRSuperUserNoAuthConsumerType.class );
    String provider = NoAuthenticationAuthenticationProvider.NO_AUTH_ID;
    if ( config.getConfigProperties().containsKey( SUPER_USER ) ) {
      provider = config.getConfigProperties().getProperty( SUPER_USER );
    }
    AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
    new PropertyAuthenticationProviderParser( config.getConfigProperties(), manager ).process( PROVIDER_LIST );
    AuthenticationPerformer<HadoopAuthorizationService, Void> performer =
        manager.getAuthenticationPerformer( HadoopAuthorizationService.class, Void.class, provider );
    if ( performer == null ) {
      throw new RuntimeException( "Unable to find relevant provider for MapR super user (id of "
          + config.getConfigProperties().getProperty( SUPER_USER ) );
    } else {
      HadoopAuthorizationService hadoopAuthorizationService = performer.perform( null );
      if ( hadoopAuthorizationService == null ) {
        throw new RuntimeException( "Unable to get HadoopAuthorizationService for provider "
            + config.getConfigProperties().getProperty( SUPER_USER ) );
      }
      for ( PentahoHadoopShim shim : config.getAvailableShims() ) {
        if ( HasHadoopAuthorizationService.class.isInstance( shim ) ) {
          ( (HasHadoopAuthorizationService) shim ).setHadoopAuthorizationService( hadoopAuthorizationService );
        } else {
          throw new Exception( "Found shim: " + shim + " that didn't implement "
              + HasHadoopAuthorizationService.class.getCanonicalName() );
        }
      }
    }
    super.onLoad( config, fsm );
  }
}
