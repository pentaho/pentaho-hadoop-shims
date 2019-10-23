/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common.osgi.jaas;

import com.sun.security.auth.login.ConfigFile;
import org.apache.karaf.jaas.config.JaasRealm;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.pentaho.di.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class used to append jaas configuration set via <code>java.security.auth.login.config</code> system property to
 * currently active jaas configuration.
 */
public class JaasRealmsRegistrar {
  private static final Logger LOGGER = LoggerFactory.getLogger( JaasRealmsRegistrar.class );
  private BundleContext bundleContext;

  public JaasRealmsRegistrar( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
  }

  public void setRealms( String configFile ) {
    try {
      HashMap<String, LinkedList<AppConfigurationEntry>> configs = new HashMap<>();

      configs.putAll( getOverridenDefaultConfigs() );

      configs.putAll( getMaprJaasConfig( configFile ) );

      List<ServiceRegistration> realmRegistrations = new ArrayList<>( configs.size() );

      for ( final Map.Entry entry : configs.entrySet() ) {
        JaasRealm realm = createJaasRealm( entry.getKey().toString(), (List) entry.getValue() );
        ServiceRegistration reg = getBundleContext().registerService( JaasRealm.class.getCanonicalName(), realm, null );
        realmRegistrations.add( reg );
      }

      String debugMessage = String.format( "Registered %s JAAS realms using system properties.", realmRegistrations.size() );
      LOGGER.debug( debugMessage );
    } catch ( Exception e ) {
      LOGGER.error( "Error during setting up MapR JAAS configuration", e );
    }
  }

  private HashMap<String, LinkedList<AppConfigurationEntry>> getOverridenDefaultConfigs() {
    HashMap<String, LinkedList<AppConfigurationEntry>> configs = new HashMap<>();

    HashMap<String, String> options = new HashMap<>();
    LinkedList<AppConfigurationEntry> entries = new LinkedList<>();

    options.put( "useTicketCache", "true" );
    options.put( "doNotPrompt", "true" );
    entries.add( new AppConfigurationEntry( "com.sun.security.auth.module.Krb5LoginModule",
      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options ) );
    configs.put( "com.sun.security.jgss.krb5.initiate", entries );

    return configs;
  }

  BundleContext getBundleContext() {
    return bundleContext;
  }

  private HashMap<String, LinkedList<AppConfigurationEntry>> getMaprJaasConfig( String configFile ) {
    try {
      if ( Utils.isEmpty( System.getProperty( "java.security.auth.login.config" ) ) ) {
        getClass().getClassLoader().loadClass( "com.mapr.baseutils.JVMProperties" ).newInstance();
      }

      Object config;
      if ( configFile == null ) {
        config = new ConfigFile();
      } else {
        config = new ConfigFile( bundleContext.getBundle().getResource( configFile ).toURI() );
      }
      try {
        Field spi = config.getClass().getDeclaredField( "spi" );
        boolean accessible = spi.isAccessible();
        spi.setAccessible( true );
        config = spi.get( config );
        spi.setAccessible( accessible );
      } catch ( NoSuchFieldException | IllegalAccessException e ) {
        // ignore
      }
      Field f = config.getClass().getDeclaredField( "configuration" );

      HashMap<String, LinkedList<AppConfigurationEntry>> configs;
      boolean accessible = f.isAccessible();
      if ( !accessible ) {
        f.setAccessible( true );
      }
      try {
        configs = (HashMap) f.get( config );
      } finally {
        if ( !accessible ) {
          f.setAccessible( accessible );
        }
      }

      if ( configs == null ) {
        throw new IllegalArgumentException( "JAAS configuration is not available" );
      }

      return configs;
    } catch ( Exception e ) {
      throw new IllegalStateException( "JAAS configuration could not be loaded at this time", e );
    }
  }

  private JaasRealm createJaasRealm( final String realmName, final List<AppConfigurationEntry> entries ) {
    return new JaasRealm() {
      @Override
      public String getName() {
        return realmName;
      }

      @Override
      public int getRank() {
        return 0;
      }

      @Override
      public AppConfigurationEntry[] getEntries() {
        return entries.toArray( new AppConfigurationEntry[ entries.size() ] );
      }
    };
  }
}
