package org.pentaho.hadoop.shim;

import org.pentaho.di.core.Const;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ShimConfigsLoader {
  private static final String CONFIGS_DIR_PREFIX = "metastore" + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";

  public static Properties loadConfigProperties( String additionalPath ) {
    return getConfigProperties(
      getURLToResourceFile( ClusterConfigNames.CONFIGS_PROP.toString(), additionalPath ) );
  }

  public static URL getURLToResourceFile( String siteFileName, String additionalPath ) {
    try {
      if ( additionalPath != null ) {
        Path currentPath = Paths.get(
          Const.getKettleDirectory() + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath + File.separator
            + siteFileName );

        if ( Files.exists( currentPath ) ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }

        currentPath = Paths.get(
          Const.getUserHomeDirectory() + File.separator + ".pentaho" + File.separator + CONFIGS_DIR_PREFIX + File.separator + additionalPath + File.separator
            + siteFileName );
        if ( Files.exists( currentPath ) ) {
          return currentPath.toAbsolutePath().toFile().toURI().toURL();
        }
      }
    } catch ( MalformedURLException ex ) {
      ex.printStackTrace();
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
        properties.load( new FileInputStream( pathToConfigProperties.getFile() ) );
      }
    } catch ( IOException ex ) {
      ex.printStackTrace();
    }

    return properties;
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

    public String toString() {
      return this.configName;
    }
  }
}
