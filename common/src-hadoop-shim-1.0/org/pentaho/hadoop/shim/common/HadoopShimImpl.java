/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hdfs.vfs.HDFSFileProvider;

import java.io.IOException;
import java.net.URI;

public class HadoopShimImpl extends CommonHadoopShim {

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

  @Override
  public RunningJob submitJob( org.pentaho.hadoop.shim.api.Configuration c ) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return c.submit();
    } catch ( InterruptedException | ClassNotFoundException e ) {
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
      return new ConfigurationProxyV2();
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to create configuration for new mapreduce api: ", e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

}
