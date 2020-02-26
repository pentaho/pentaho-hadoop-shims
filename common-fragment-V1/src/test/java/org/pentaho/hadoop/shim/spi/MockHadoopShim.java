/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.spi;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;

import java.io.IOException;
import java.sql.Driver;
import java.util.List;

public class MockHadoopShim implements HadoopShim {

  @Override
  public String[] getNamenodeConnectionInfo( Configuration c ) {
    return null;
  }

  @Override
  public String[] getJobtrackerConnectionInfo( Configuration c ) {
    return null;
  }

  @Override
  public String getHadoopVersion() {
    return null;
  }

  @Override
  public Configuration createConfiguration() {
    return null;
  }

  @Override
  public Configuration createConfiguration( String namedCluster ) {
    return null;
  }

  @Override
  public Configuration createConfiguration( NamedCluster namedCluster ) {
    return null;
  }

  @Override
  public FileSystem getFileSystem( Configuration conf ) throws IOException {
    return null;
  }

  @Override
  public Driver getHiveJdbcDriver() {
    return null;
  }

  @Override
  public Driver getJdbcDriver( String driverType ) {
    return null;
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                              String jobtrackerPort, Configuration conf, List<String> logMessages )
    throws Exception {
  }

  @Override
  public DistributedCacheUtil getDistributedCacheUtil() {
    return null;
  }

  @Override
  public RunningJob submitJob( Configuration c ) throws IOException {
    return null;
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[ 0 ];
  }

  @Override
  public Class<?> getHadoopWritableCompatibleClass( ValueMetaInterface kettleType ) {
    return null;
  }

  @Override
  public String getPentahoMapReduceCombinerClass() {
    return null;
  }

  @Override
  public String getPentahoMapReduceReducerClass() {
    return null;
  }

  @Override
  public String getPentahoMapReduceMapRunnerClass() {
    return null;
  }

}
