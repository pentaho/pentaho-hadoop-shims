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

package org.pentaho.hadoop.shim.mapr31.authorization;

import java.net.URLClassLoader;
import java.util.Properties;

import org.pentaho.di.core.auth.AuthenticationConsumerPluginType;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.mapr31.authentication.MapRSuperUserNoAuthConsumer.MapRSuperUserNoAuthConsumerType;
import org.pentaho.hadoop.shim.mapr31.authentication.PropertyAuthenticationProviderParser;
import org.pentaho.hadoop.shim.mapr31.delegatingShims.DelegatingHadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

public class AuthenticatingHadoopShim extends DelegatingHadoopShim {
  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    String activators = config.getConfigProperties().getProperty( "activator.classes" );
    if ( activators != null ) {
      activators = activators.trim();
      for ( String className : activators.split( "," ) ) {
        className = className.trim();
        if ( className.length() > 0 ) {
          try {
            Class.forName( className ).newInstance();
          } catch ( Exception e ) {
            LogChannel.GENERAL.logError( e.getMessage(), e );
          }
        }
      }
    }
    AuthenticationConsumerPluginType.getInstance().registerPlugin( (URLClassLoader) getClass().getClassLoader(),
        MapRSuperUserNoAuthConsumerType.class );
    String provider = NoAuthenticationAuthenticationProvider.NO_AUTH_ID;
    if ( config.getConfigProperties().containsKey( SUPER_USER ) ) {
      provider = config.getConfigProperties().getProperty( SUPER_USER );
    }
    AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
    new PropertyAuthenticationProviderParser( config.getConfigProperties(), manager ).process( PROVIDER_LIST );
    AuthenticationPerformer<HadoopAuthorizationService, Properties> performer =
        manager.getAuthenticationPerformer( HadoopAuthorizationService.class, Properties.class, provider );
    if ( performer == null ) {
      throw new RuntimeException( "Unable to find relevant provider for MapR super user (id of "
          + config.getConfigProperties().getProperty( SUPER_USER ) );
    } else {
      HadoopAuthorizationService hadoopAuthorizationService = performer.perform( config.getConfigProperties() );
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
