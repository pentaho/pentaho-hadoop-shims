/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.api.shims;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class LegacyShimLocator {

  private static final String ACTIVE_HADOOP_CONFIGURATION = "active.hadoop.configuration";
  private static final String HADOOP_CONFIGURATIONS_PATH = "hadoop.configurations.path";
  private static final String BIG_DATA_PLUGIN_PROPERTIES = "plugin.properties";
  public static final String HADOOP_SPOON_PLUGIN = "HadoopSpoonPlugin";
  private List<ShimIdentifierInterface> registeredShims;
  private static LegacyShimLocator instance;

  public LegacyShimLocator( List<ShimIdentifierInterface> shims ) {
    getInstance().registeredShims = shims;
  }

  private LegacyShimLocator() {
  }

  public static LegacyShimLocator getInstance() {
    if ( instance == null ) {
      instance = new LegacyShimLocator();
    }
    return instance;
  }

  public List<ShimIdentifierInterface> getRegisteredShims() {
    return registeredShims;
  }

  public static String getLegacyDefaultShimName() throws IOException {
    PluginInterface pluginInterface =
      PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, HADOOP_SPOON_PLUGIN );
    Properties legacyProperties;

    try {
      legacyProperties = loadProperties( pluginInterface, BIG_DATA_PLUGIN_PROPERTIES );
      return legacyProperties.getProperty( ACTIVE_HADOOP_CONFIGURATION );
    } catch ( KettleFileException | NullPointerException e ) {
      throw new IOException( e );
    }
  }

  public static String getLegacyDefaultShimDir( String shimFolder ) throws IOException {
    PluginInterface pluginInterface =
      PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, HADOOP_SPOON_PLUGIN );
    Properties legacyProperties;

    try {
      legacyProperties = loadProperties( pluginInterface, BIG_DATA_PLUGIN_PROPERTIES );
      String legacyShimsFolder = legacyProperties.getProperty( HADOOP_CONFIGURATIONS_PATH );
      FileObject shimDirectoryObject =
        KettleVFS.getFileObject( pluginInterface.getPluginDirectory().getPath() + Const.FILE_SEPARATOR
          + legacyShimsFolder + Const.FILE_SEPARATOR + shimFolder );
      return shimDirectoryObject.getURL().getPath();
    } catch ( KettleFileException | NullPointerException e ) {
      throw new IOException( e );
    }
  }

  /**
   * Get the values from big data plugin plugin.properties
   *
   * @return Properties object containing the values from big data plugin plugin.properties
   */
  public static Properties getLegacyBigDataProps() {
    PluginInterface pluginInterface =
      PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, HADOOP_SPOON_PLUGIN );

    try {
      return loadProperties( pluginInterface, BIG_DATA_PLUGIN_PROPERTIES );
    } catch ( KettleFileException | IOException e ) {
      return new Properties();
    }
  }

  /**
   * Loads a properties file from the plugin directory for the plugin interface provided
   *
   * @param plugin
   * @return
   * @throws KettleFileException
   * @throws IOException
   */
  private static Properties loadProperties( PluginInterface plugin, String relativeName ) throws KettleFileException,
    IOException {
    if ( plugin == null ) {
      throw new NullPointerException();
    }
    FileObject propFile =
      KettleVFS.getFileObject( plugin.getPluginDirectory().getPath() + Const.FILE_SEPARATOR + relativeName );
    if ( !propFile.exists() ) {
      throw new FileNotFoundException( propFile.toString() );
    }
    try {
      Properties pluginProperties = new Properties();
      pluginProperties.load( new FileInputStream( propFile.getName().getPath() ) );
      return pluginProperties;
    } catch ( Exception e ) {
      // Do not catch ConfigurationException. Different shims will use different
      // packages for this exception.
      throw new IOException( e );
    }
  }
}
