/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common.delegating;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.common.authorization.HasHadoopAuthorizationService;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URI;
import java.sql.Driver;
import java.util.List;

public class DelegatingHadoopShim implements HadoopShim, HasHadoopAuthorizationService {
  public static final String SUPER_USER = "authentication.superuser.provider";
  public static final String PROVIDER_LIST = "authentication.provider.list";
  private HadoopShim delegate = null;

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    delegate.onLoad( config, fsm );
  }

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) throws Exception {
    delegate = hadoopAuthorizationService.getShim( HadoopShim.class );
  }

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public Driver getHiveJdbcDriver() {
    return delegate.getHiveJdbcDriver();
  }

  @Override
  public Driver getJdbcDriver( String driverType ) {
    return delegate.getJdbcDriver( driverType );
  }

  @Override
  public String[] getNamenodeConnectionInfo( Configuration c ) {
    return delegate.getNamenodeConnectionInfo( c );
  }

  @Override
  public String[] getJobtrackerConnectionInfo( Configuration c ) {
    return delegate.getJobtrackerConnectionInfo( c );
  }

  @Override
  public String getHadoopVersion() {
    return delegate.getHadoopVersion();
  }

  @Override
  public Configuration createConfiguration() {
    return delegate.createConfiguration();
  }

  @Override
  public FileSystem getFileSystem( Configuration conf ) throws IOException {
    return delegate.getFileSystem( conf );
  }

  @Override
  public FileSystem getFileSystem( URI uri, Configuration conf, String user ) throws IOException, InterruptedException {
    return delegate.getFileSystem( uri, conf, user );
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
      String jobtrackerPort, Configuration conf, List<String> logMessages ) throws Exception {
    delegate.configureConnectionInformation( namenodeHost, namenodePort, jobtrackerHost, jobtrackerPort, conf,
        logMessages );
  }

  @Override
  public DistributedCacheUtil getDistributedCacheUtil() throws ConfigurationException {
    return delegate.getDistributedCacheUtil();
  }

  @Override
  public RunningJob submitJob( Configuration c ) throws IOException {
    return delegate.submitJob( c );
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public Class<?> getHadoopWritableCompatibleClass( ValueMetaInterface kettleType ) {
    return delegate.getHadoopWritableCompatibleClass( kettleType );
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public Class<?> getPentahoMapReduceCombinerClass() {
    return delegate.getPentahoMapReduceCombinerClass();
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public Class<?> getPentahoMapReduceReducerClass() {
    return delegate.getPentahoMapReduceReducerClass();
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public Class<?> getPentahoMapReduceMapRunnerClass() {
    return delegate.getPentahoMapReduceMapRunnerClass();
  }
}
