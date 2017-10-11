/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.authorization;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.auth.AuthenticationConsumerPluginType;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.common.authentication.HadoopNoAuthConsumer;
import org.pentaho.hadoop.shim.common.authentication.PropertyAuthenticationProviderParser;
import org.pentaho.hadoop.shim.common.delegating.DelegatingHadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;

import java.net.URLClassLoader;
import java.util.Properties;

public class AuthenticatingHadoopShim extends DelegatingHadoopShim {

  public static final String MAPPING_IMPERSONATION_TYPE = "pentaho.authentication.default.mapping.impersonation.type";

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    AuthenticationConsumerPluginType.getInstance().registerPlugin( (URLClassLoader) getClass().getClassLoader(),
      HadoopNoAuthConsumer.HadoopNoAuthConsumerType.class );
    String activators = config.getConfigProperties().getProperty( "activator.classes" );
    if ( activators != null ) {
      activators = activators.trim();
      for ( String className : activators.split( "," ) ) {
        className = className.trim();
        if ( className.length() > 0 ) {
          createActivatorInstance( className );
        }
      }
    }
    String provider = NoAuthenticationAuthenticationProvider.NO_AUTH_ID;
    if ( config.getConfigProperties().containsKey( SUPER_USER ) && !config.getConfigProperties()
      .getProperty( MAPPING_IMPERSONATION_TYPE, "" ).trim().equalsIgnoreCase( "disabled" ) ) {
      provider = config.getConfigProperties().getProperty( SUPER_USER );
      if ( provider.trim().length() == 0 ) {
        provider = NoAuthenticationAuthenticationProvider.NO_AUTH_ID;
      }
    }
    AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
    new PropertyAuthenticationProviderParser( config.getConfigProperties(), manager ).process( PROVIDER_LIST );
    AuthenticationPerformer<HadoopAuthorizationService, Properties> performer =
      manager.getAuthenticationPerformer( HadoopAuthorizationService.class, Properties.class, provider );
    if ( performer == null ) {
      throw new RuntimeException( "Unable to find relevant provider for chosen authentication method (id of "
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

  @VisibleForTesting
  void createActivatorInstance( String className ) {
    try {
      Class.forName( className ).newInstance();
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( e.getMessage(), e );
    }
  }
}
