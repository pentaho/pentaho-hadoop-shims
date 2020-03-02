/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common;

import org.pentaho.hadoop.shim.ShimException;
import org.pentaho.hadoop.shim.ShimRuntimeException;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;

import java.io.IOException;
import java.util.List;

public class HadoopShimImpl extends CommonHadoopShim {

  @Override
  protected String getDefaultNamenodePort() {
    return "8020";
  }

  @Override
  protected String getDefaultJobtrackerPort() {
    return "8021";
  }

  @Override
  public RunningJob submitJob( org.pentaho.hadoop.shim.api.internal.Configuration c ) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return c.submit();
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      throw new ShimRuntimeException( e );
    } catch ( ClassNotFoundException e ) {
      throw new ShimRuntimeException( e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public org.pentaho.hadoop.shim.api.internal.Configuration createConfiguration() {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new ConfigurationProxyV2();
    } catch ( IOException e ) {
      throw new ShimRuntimeException( "Unable to create configuration for new mapreduce api: ", e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public org.pentaho.hadoop.shim.api.internal.Configuration createConfiguration( String namedCluster ) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new ConfigurationProxyV2( namedCluster );
    } catch ( IOException e ) {
      throw new ShimRuntimeException( "Unable to create configuration for new mapreduce api: ", e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                              String jobtrackerPort,
                                              org.pentaho.hadoop.shim.api.internal.Configuration conf,
                                              List<String> logMessages ) throws Exception {

    String runtimeFsDefaultName = conf.get( "pentaho.runtime.fs.default.name" );
    String runtimeFsDefaultScheme = conf.get( "pentaho.runtime.fs.default.scheme", "hdfs" );
    String runtimeJobTracker = conf.get( "pentaho.runtime.job.tracker" );
    if ( runtimeFsDefaultName == null ) {
      if ( namenodeHost == null || namenodeHost.trim().length() == 0 ) {
        throw new ShimException( "No hdfs host specified!" );
      }

      if ( namenodePort != null
        && namenodePort.trim().length() != 0
        && !"-1".equals( namenodePort.trim() ) ) {
        namenodePort = ":" + namenodePort;
      } else {
        // it's been realized that this is pretty fine to have
        // NameNode URL w/o port: e.g. HA mode (BAD-358)
        namenodePort = "";
        logMessages.add( "No hdfs port specified - HA? " );
      }

      runtimeFsDefaultName = runtimeFsDefaultScheme + "://" + namenodeHost + namenodePort;
    }

    if ( runtimeJobTracker == null ) {
      if ( jobtrackerHost == null || jobtrackerHost.trim().length() == 0 ) {
        throw new ShimException( "No job tracker host specified!" );
      }

      if ( jobtrackerPort == null || jobtrackerPort.trim().length() == 0 ) {
        jobtrackerPort = getDefaultJobtrackerPort();
        logMessages.add( "No job tracker port specified - using default: " + jobtrackerPort );
      }
      runtimeJobTracker = jobtrackerHost + ":" + jobtrackerPort;
    }

    conf.set( "fs.default.name", runtimeFsDefaultName );
    conf.set( "mapred.job.tracker", runtimeJobTracker );
  }
}
