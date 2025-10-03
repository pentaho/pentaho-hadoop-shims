/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop.shim;

import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ShimConfigsLoader {

  private static final Class<?> PKG = ShimConfigsLoader.class; // for i18n purposes, needed by Translator2!!
  private static LogChannelInterface log = new LogChannel( ShimConfigsLoader.class.getName() );

  public static Set<String> CLUSTER_NAME_FOR_LOGGING = new HashSet<String>();
  public static Set<String> SITE_FILE_NAME = new HashSet<String>();

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
      FileObject currentPath = null;
      if ( additionalPath != null && !additionalPath.equals( "" ) ) {
        currentPath = KettleVFS.getFileObject(
          Const.getKettleDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath
            + File.separator
            + siteFileName );

        if ( currentPath.exists() ) {
          return currentPath.getURL();
        }

        currentPath = KettleVFS.getFileObject(
          Const.getUserHomeDirectory() + File.separator + ".pentaho" + File.separator + CONFIGS_DIR_PREFIX
            + File.separator + additionalPath + File.separator
            + siteFileName );
        if ( currentPath.exists() ) {
          return currentPath.getURL();
        }

        currentPath = KettleVFS.getFileObject(
          Const.getUserHomeDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath
            + File.separator
            + siteFileName );
        if ( currentPath.exists() ) {
          return currentPath.getURL();
        }

        // normal metastore locations failed, see if there's a metastore in the big-data-plugin folder
        // this should only exist if this instance of pentaho were created to run on a yarn cluster
        PluginInterface pluginInterface =
          PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, "HadoopSpoonPlugin" );
        currentPath = KettleVFS.getFileObject( pluginInterface.getPluginDirectory().getPath()
          + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath + File.separator + siteFileName );
        if ( currentPath.exists() ) {
          return currentPath.getURL();
        }
      }
      // cluster name was missing or else config files were not found; try looking for a legacy configuration
      String defaultShim = LegacyShimLocator.getLegacyDefaultShimName();
      List<ShimIdentifierInterface> shimIdentifers = LegacyShimLocator.getInstance().getRegisteredShims();
      for ( ShimIdentifierInterface shim : shimIdentifers ) {
        if ( shim.getId().equals( defaultShim ) ) {
          // only return the legacy folder if the shim still exists
          currentPath = KettleVFS.getFileObject(
            LegacyShimLocator.getLegacyDefaultShimDir( defaultShim ) + File.separator + siteFileName );
          if ( currentPath.exists() ) {
            log.logBasic( BaseMessages.getString( PKG, "ShimConfigsLoader.UsingLegacyConfig" ) );
            return currentPath.getURL();
          }
        }
      }

      // Work around to avoid multiple logging for VFS
      // Don't report if the cluster had no name
      if ( additionalPath != null && !"".equals( additionalPath ) ) {
        if ( !CLUSTER_NAME_FOR_LOGGING.contains( additionalPath ) ) {
          SITE_FILE_NAME.clear();
          log.logBasic( BaseMessages.getString( PKG, "ShimConfigsLoader.UnableToFindConfigs" ), siteFileName,
            additionalPath );
          CLUSTER_NAME_FOR_LOGGING.add( additionalPath );
          SITE_FILE_NAME.add( siteFileName );
        } else if ( !SITE_FILE_NAME.contains( siteFileName ) ) {
          log.logBasic( BaseMessages.getString( PKG, "ShimConfigsLoader.UnableToFindConfigs" ), siteFileName,
            additionalPath );
          SITE_FILE_NAME.add( siteFileName );
        }
      }

    } catch ( KettleFileException | IOException ex ) {
      log.logError( BaseMessages.getString( PKG, "ShimConfigsLoader.ExceptionReadingFile" ),
        siteFileName, additionalPath, ex.getStackTrace() );
    }
    return null;
  }

  public static void addConfigsAsResources( NamedCluster namedCluster,
                                            BiConsumer<? super InputStream, ? super String> configurationConsumer ) {

    addConfigsAsResources( namedCluster, configurationConsumer, createSiteFilesArray() );
  }

  public static void addConfigsAsResources( NamedCluster namedCluster,
                                            BiConsumer<? super InputStream, ? super String> configurationConsumer,
                                            String... fileNames ) {
    addConfigsAsResources( namedCluster, configurationConsumer, Arrays.asList( fileNames ) );
  }

  public static void addConfigsAsResources( NamedCluster namedCluster,
                                            BiConsumer<? super InputStream, ? super String> configurationConsumer,
                                            List<String> fileNames ) {

    for ( String siteFile : fileNames ) {
      InputStream is = namedCluster.getSiteFileInputStream( siteFile );
      if ( is != null ) {
        configurationConsumer.accept( is, siteFile );
      }
    }
  }

  /**
   * @deprecated Use {@Link addConfigsAsResources(NamedCluster namedCluster,
   * BiConsumer < ? super InputStream, ? super String > configurationConsumer)}
   */
  @Deprecated
  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer ) {
    addConfigsAsResources( additionalPath, configurationConsumer, createSiteFilesArray() );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            String... fileNames ) {
    addConfigsAsResources( additionalPath, configurationConsumer, Arrays.asList( fileNames ) );
  }

  public static void addConfigsAsResources( String additionalPath, Consumer<? super URL> configurationConsumer,
                                            ClusterConfigNames... fileNames ) {
    setSystemProperties( additionalPath );

    addConfigsAsResources( additionalPath, configurationConsumer,
      Arrays.stream( fileNames ).map( ClusterConfigNames::toString ).collect( Collectors.toList() ) );
  }

  public static void setSystemProperties( String additionalPath ) {
    Properties properties = loadConfigProperties( additionalPath );
    for ( String propertyName : properties.stringPropertyNames() ) {
      if ( propertyName.startsWith( "java.system." ) ) {
        System.setProperty( propertyName.substring( "java.system.".length() ),
          properties.get( propertyName ).toString() );
      }
    }
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
      log.logError( BaseMessages.getString( ShimConfigsLoader.class, "ShimConfigsLoader.ExceptionLoadingProperties" ),
        ex );
    }

    return properties;
  }

  public static Map<String, String> parseFile( URL fileUrl ) {
    Configuration c = new Configuration();
    c.addResource( fileUrl );
    return c.getValByRegex( ".*" );
  }

  public static Map<String, String> parseFile( NamedCluster namedCluster, String fileName ) {
    Configuration c = new Configuration();
    if ( namedCluster != null ) {
      InputStream is = namedCluster.getSiteFileInputStream( fileName );
      if ( is != null ) {
        c.addResource( is, fileName );
        return c.getValByRegex( ".*" );
      }
    }
    return null;
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

