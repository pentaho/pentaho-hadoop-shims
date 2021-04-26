/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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


import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.Versioned;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mapreduce.JobUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.FastLongHistogram;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.htrace.core.Tracer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;
import org.pentaho.hadoop.shim.common.ShimUtils;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public class  HadoopShim extends HadoopShimImpl {

  private static final String FS_HDFS_IMPL = "fs.hdfs.impl";
  private static final String FS_FILE_IMPL = "fs.file.impl";
  private static final String IPC_CLIENT_CONNECT_TIMEOUT = "ipc.client.connect.max.retries.on.timeouts";

  public HadoopShim() {
    super();
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[] {
      HConstants.class, org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class,
      org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.class, Put.class,
      RpcServer.class, CompatibilityFactory.class, JobUtil.class, TableMapper.class, FastLongHistogram.class,
      Snapshot.class, ZooKeeper.class, Channel.class, Message.class, UnsafeByteOperations.class, Lists.class,
      Tracer.class, MetricRegistry.class, ArrayUtils.class, ObjectMapper.class, Versioned.class,
      JsonView.class, ZKWatcher.class, CacheLoader.class
    };
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                              String jobtrackerPort, Configuration conf,
                                              List<String> logMessages ) throws Exception {
    //Do nothing
  }

  @Override
  public FileSystem getFileSystem( Configuration conf ) throws IOException {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    conf.set( FS_HDFS_IMPL, org.apache.hadoop.fs.azure.NativeAzureFileSystem.class.getName() );
    conf.set( FS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName() );
    Properties legacyPluginProps = LegacyShimLocator.getLegacyBigDataProps();
    String timeoutNum = legacyPluginProps.getProperty( HADOOPFS_IPC_CLIENT_CONNECT_MAX_RETRIES_ON_TIMEOUTS );
    if ( null != timeoutNum && !timeoutNum.equals( "" ) ) {
      conf.set( IPC_CLIENT_CONNECT_TIMEOUT, timeoutNum );
    }
    try ( FileSystemProxy fsp = new FileSystemProxy(
      org.apache.hadoop.fs.FileSystem.get( ShimUtils.asConfiguration( conf ) ) ) ) {
      return fsp;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public FileSystem getFileSystem( URI uri, Configuration conf, String user ) throws IOException, InterruptedException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    conf.set( FS_HDFS_IMPL, org.apache.hadoop.fs.azure.NativeAzureFileSystem.class.getName() );
    conf.set( FS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName() );
    Properties legacyPluginProps = LegacyShimLocator.getLegacyBigDataProps();
    String timeoutNum = legacyPluginProps.getProperty( HADOOPFS_IPC_CLIENT_CONNECT_MAX_RETRIES_ON_TIMEOUTS );
    if ( null != timeoutNum && !timeoutNum.equals( "" ) ) {
      conf.set( IPC_CLIENT_CONNECT_TIMEOUT, timeoutNum );
    }
    try ( FileSystemProxy fsp = new FileSystemProxy(
      org.apache.hadoop.fs.FileSystem.get( uri, ShimUtils.asConfiguration( conf ), user ) ) ) {
      return fsp;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public FileSystem getFileSystem( URI uri, Configuration conf, NamedCluster namedCluster ) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    conf.set( FS_HDFS_IMPL, org.apache.hadoop.fs.azure.NativeAzureFileSystem.class.getName() );
    conf.set( FS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName() );
    Properties legacyPluginProps = LegacyShimLocator.getLegacyBigDataProps();
    String timeoutNum = legacyPluginProps.getProperty( HADOOPFS_IPC_CLIENT_CONNECT_MAX_RETRIES_ON_TIMEOUTS );
    if ( null != timeoutNum && !timeoutNum.equals( "" ) ) {
      conf.set( IPC_CLIENT_CONNECT_TIMEOUT, timeoutNum );
    }
    try ( FileSystemProxy fsp = new FileSystemProxy(
      org.apache.hadoop.fs.FileSystem.get( uri, ShimUtils.asConfiguration( conf ) ) ) ) {
      return fsp;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

}
