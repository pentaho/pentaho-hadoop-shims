/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.cdh54;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.DatabasePluginType;
import org.pentaho.di.core.plugins.Plugin;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;
import org.pentaho.hadoop.shim.common.DistributedCacheUtilImpl;
import org.pentaho.hadoop.shim.common.DriverProxyInvocationChain;
import org.pentaho.hdfs.vfs.HDFSFileProvider;
import org.pentaho.reporting.libraries.base.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HadoopShim extends CommonHadoopShim {

  static {
    JDBC_DRIVER_MAP.put( "hive2", org.apache.hive.jdbc.HiveDriver.class );
  }

  @Override
  protected String getDefaultNamenodePort() {
    return "8020";
  }

  @Override
  protected String getDefaultJobtrackerPort() {
    return "8021";
  }

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    registerExtraDatabaseTypes( config.getConfigProperties() );
    fsm.addProvider( config, "hdfs", config.getIdentifier(), new HDFSFileProvider() );
    setDistributedCacheUtil( new DistributedCacheUtilImpl( config ) {

      public void addFileToClassPath( Path file, Configuration conf ) throws IOException {
        String classpath = conf.get( "mapred.job.classpath.files" );
        conf.set( "mapred.job.classpath.files",
          classpath == null ? file.toString() : classpath + getClusterPathSeparator() + file.toString() );
        FileSystem fs = FileSystem.get( conf );
        URI uri = fs.makeQualified( file ).toUri();

        DistributedCache.addCacheFile( uri, conf );
      }

      public String getClusterPathSeparator() {
        // Use a comma rather than an OS-specific separator (see https://issues.apache.org/jira/browse/HADOOP-4864)
        return System.getProperty( "hadoop.cluster.path.separator", "," );
      }
    } );
  }

  protected void registerExtraDatabaseTypes( Properties configuration ) throws KettlePluginException {
    /*String hiveSimbaDriverName = configuration.getProperty( "hive2.simba.driver", "com.simba.hive.jdbc41.HS2Driver" );
      JDBC_POSSIBLE_DRIVER_MAP.put( "hive2Simba", hiveSimbaDriverName );*/

    String impalaSimbaDriverName =
      configuration.getProperty( "impala.simba.driver", "com.cloudera.impala.jdbc41.Driver" );
    JDBC_POSSIBLE_DRIVER_MAP.put( "ImpalaSimba", impalaSimbaDriverName );
  }

  protected void registerExtraDatabaseType( String id, String description, String mainClass )
    throws KettlePluginException {
    Map<Class<?>, String> classMap = new HashMap<Class<?>, String>();
    classMap.put( DatabaseInterface.class, mainClass );
    PluginInterface dbPlugin =
      new Plugin(
        new String[] { id }, DatabasePluginType.class, DatabaseInterface.class, "", description, description, null,
        false,
        false, classMap, new ArrayList<String>(), null, null, null, null, null );
    PluginRegistry.getInstance().addClassLoader(
      (URLClassLoader) Thread.currentThread().getContextClassLoader().getParent(), dbPlugin );
    PluginRegistry.getInstance().registerPlugin( DatabasePluginType.class, dbPlugin );
  }

  @Override
  public RunningJob submitJob( org.pentaho.hadoop.shim.api.Configuration c ) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      Job job = ( (org.pentaho.hadoop.shim.cdh54.ConfigurationProxyV2) c ).getJob();
      job.submit();
      return new RunningJobProxyV2( job );
    } catch ( InterruptedException e ) {
      throw new RuntimeException( e );
    } catch ( ClassNotFoundException e ) {
      throw new RuntimeException( e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public org.pentaho.hadoop.shim.api.Configuration createConfiguration() {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new org.pentaho.hadoop.shim.cdh54.ConfigurationProxyV2();
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to create configuration for new mapreduce api: ", e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

}
