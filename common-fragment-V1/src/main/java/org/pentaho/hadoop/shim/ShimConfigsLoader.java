/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.api.ShimIdentifierInterface;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ShimConfigsLoader {

  private static final Class<?> PKG = ShimConfigsLoader.class; // for i18n purposes, needed by Translator2!!
  private static LogChannelInterface log = new LogChannel( ShimConfigsLoader.class.getName() );

  public static final String CONFIGS_DIR_PREFIX =
    "metastore" + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";

  public static Properties loadConfigProperties( String additionalPath ) {
    return getConfigProperties(
      getURLToResourceFile( ClusterConfigNames.CONFIGS_PROP.toString(), additionalPath ) );
  }

  // complexity rule suppressed because the level of nesting is not significant and moving logic to other methods
  // would make it more difficult to trace
  @SuppressWarnings( "squid:S3776" )
  public static URL getURLToResourceFile( String siteFileName, String additionalPath ) {
    try {
      Path currentPath = null;
      if ( additionalPath != null && !additionalPath.equals( "" ) ) {
        currentPath = Paths.get(
          Const.getKettleDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath
            + File.separator
            + siteFileName );

        if ( currentPath.toFile().exists() ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        currentPath = Paths.get(
          Const.getUserHomeDirectory() + File.separator + ".pentaho" + File.separator + CONFIGS_DIR_PREFIX
            + File.separator + additionalPath + File.separator
            + siteFileName );
        if ( currentPath.toFile().exists() ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        currentPath = Paths.get(
          Const.getUserHomeDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath
            + File.separator
            + siteFileName );
        if ( currentPath.toFile().exists() ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        // normal metastore locations failed, see if there's a metastore in the big-data-plugin folder
        // this should only exist if this instance of pentaho were created to run on a yarn cluster
        PluginInterface pluginInterface =
          PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, "HadoopSpoonPlugin" );
        currentPath = Paths.get( pluginInterface.getPluginDirectory().getPath()
          + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath + File.separator + siteFileName );
        if ( currentPath.toFile().exists() ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }
      }
      // cluster name was missing or else config files were not found; try looking for a legacy configuration
      String defaultShim = LegacyShimLocator.getLegacyDefaultShimName();
      List<ShimIdentifierInterface> shimIdentifers = PentahoSystem.getAll( ShimIdentifierInterface.class );
      for ( ShimIdentifierInterface shim : shimIdentifers ) {
        if ( shim.getId().equals( defaultShim ) ) {
          // only return the legacy folder if the shim still exists
          currentPath = Paths.get( LegacyShimLocator.getLegacyDefaultShimDir( defaultShim ).toString() + File.separator + siteFileName );
          if ( currentPath.toFile().exists() ) {
            log.logBasic( BaseMessages.getString( PKG, "ShimConfigsLoader.UsingLegacyConfig" ) );
            return currentPath.toAbsolutePath().toFile().toURI().toURL();
          }
        }
      }
      log.logError( BaseMessages.getString( PKG, "ShimConfigsLoader.UnableToFindConfigs" ),
        siteFileName, additionalPath );
    } catch ( IOException ex ) {
      log.logError( BaseMessages.getString( PKG, "ShimConfigsLoader.ExceptionReadingFile" ),
        siteFileName, additionalPath, ex.getStackTrace() );
    }
    return null;
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer ) {
    addConfigsAsResources( additionalPath, configurationConsumer, createSiteFilesArray() );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            String... fileNames ) {
    addConfigsAsResources( additionalPath, configurationConsumer, Arrays.asList( fileNames ) );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            ClusterConfigNames... fileNames ) {
    Properties properties = loadConfigProperties( additionalPath );
    for ( String propertyName : properties.stringPropertyNames() ) {
      if ( propertyName.startsWith( "java.system." ) ) {
        System.setProperty( propertyName.substring( "java.system.".length() ),
          properties.get( propertyName ).toString() );
      }
    }

    addConfigsAsResources( additionalPath, configurationConsumer,
      Arrays.stream( fileNames ).map( ClusterConfigNames::toString ).collect( Collectors.toList() ) );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            List<String> fileNames ) {
    fileNames.stream().map( siteFile -> getURLToResourceFile( siteFile, additionalPath ) )
      .filter( Objects::nonNull ).forEach( configurationConsumer );
  }

  private static String[] createSiteFilesArray() {
    return new String[] { ClusterConfigNames.CORE_SITE.toString(), ClusterConfigNames.HDFS_SITE.toString(),
      ClusterConfigNames.YARN_SITE.toString(), ClusterConfigNames.MAPRED_SITE.toString(),
      ClusterConfigNames.HBASE_SITE.toString(), ClusterConfigNames.HIVE_SITE.toString() };
  }

  private static Properties getConfigProperties( URL pathToConfigProperties ) {
    Properties properties = new Properties();

    try {
      if ( pathToConfigProperties != null ) {
        FileInputStream fis = new FileInputStream( pathToConfigProperties.getFile() );
        properties.load( fis );
        fis.close();
      }
    } catch ( IOException ex ) {
      log.logError( BaseMessages.getString( ShimConfigsLoader.class, "ShimConfigsLoader.ExceptionLoadingProperties" ), ex );
    }

    return properties;
  }

  public static Map<String, String> parseFile( URL fileUrl ) {
    Configuration c = new Configuration();
    c.addResource( fileUrl );
    return c.getValByRegex( ".*" );
  }

  public enum ClusterConfigNames {
    CONFIGS_PROP( "config.properties" ),
    HDFS_SITE( "hdfs-site.xml" ),
    CORE_SITE( "core-site.xml" ),
    HIVE_SITE( "hive-site.xml" ),
    YARN_SITE( "yarn-site.xml" ),
    HBASE_SITE( "hbase-site.xml" ),
    MAPRED_SITE( "mapred-site.xml" ),
    HBASE_DEFAULT( "hbase-default.xml" );

    private final String configName;

    ClusterConfigNames( String configName ) {
      this.configName = configName;
    }

    @Override
    public String toString() {
      return this.configName;
    }
  }
}
