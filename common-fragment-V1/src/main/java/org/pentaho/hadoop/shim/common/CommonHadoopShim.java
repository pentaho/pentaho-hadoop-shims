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

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.ShimRuntimeException;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.internal.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.fs.FileSystem;
import org.pentaho.hadoop.shim.api.internal.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class CommonHadoopShim implements HadoopShim {

  private static final String FS_HDFS_IMPL = "fs.hdfs.impl";
  private static final String FS_FILE_IMPL = "fs.file.impl";
  private static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
  private static final String SHIM_NOT_SUPPORTED_DRIVER =
    "org.pentaho.hadoop.shim.common.CommonHadoopShim$NotSupportedDriver";
  private static final String DEFAULT_NAMENODE_PORT = "9000";
  private static final String DEFAULT_JOBTRACKER_PORT = "9001";

  public static final String PENTAHO_MAPREDUCE_GENERIC_COMBINER_CLASS_NAME =
    "org.pentaho.hadoop.mapreduce.GenericTransCombiner";
  public static final String PENTAHO_MAPREDUCE_GENERIC_REDUCER_CLASS_NAME =
    "org.pentaho.hadoop.mapreduce.GenericTransReduce";
  public static final String PENTAHO_MAPREDUCE_RUNNABLE_CLASS_NAME = "org.pentaho.hadoop.mapreduce.PentahoMapRunnable";

  private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger( getClass() );

  public static class NotSupportedDriver implements Driver {
    public static final SQLException notSupported =
      new SQLException( "Chosen driver is not supported in currently active Hadoop shim" );

    @Override public Connection connect( String url, Properties info ) throws SQLException {
      throw notSupported;
    }

    @Override public boolean acceptsURL( String url ) throws SQLException {
      throw notSupported;
    }

    @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
      throw notSupported;
    }

    @Override public int getMajorVersion() {
      return 0;
    }

    @Override public int getMinorVersion() {
      return 0;
    }

    @Override public boolean jdbcCompliant() {
      return false;
    }

    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return null;
    }
  }

  private DistributedCacheUtil dcUtil;

  @SuppressWarnings( "serial" )
  protected static final Map<String, Class<? extends Driver>> JDBC_DRIVER_MAP = new HashMap<>();

  @SuppressWarnings( "serial" )
  protected static final Map<String, String> JDBC_POSSIBLE_DRIVER_MAP = new HashMap<>();

  static {
    JDBC_POSSIBLE_DRIVER_MAP.put( "hive2Simba", SHIM_NOT_SUPPORTED_DRIVER );
    JDBC_POSSIBLE_DRIVER_MAP.put( "ImpalaSimba", SHIM_NOT_SUPPORTED_DRIVER );
    JDBC_POSSIBLE_DRIVER_MAP
      .put( "SparkSqlSimba", SHIM_NOT_SUPPORTED_DRIVER );
    JDBC_POSSIBLE_DRIVER_MAP.put( "Impala", "org.apache.hive.jdbc.HiveDriver" );
  }

  @SuppressWarnings( "unchecked" )
  protected Class<? extends Driver> tryToLoadDriver( String driverClassName ) {
    Class possibleDriver = null;
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      possibleDriver = Class.forName( driverClassName );
      if ( Driver.class.isAssignableFrom( possibleDriver ) ) {
        return possibleDriver;
      } else {
        throw new ClassCastException( "Specified extra driver class does not extends java.sql.Driver" );
      }
    } catch ( ClassNotFoundException e ) {
      // Ignore
    } catch ( ClassCastException e2 ) {
      logger.error( "ClassCastException caught.", e2 );
    } finally {
      Thread.currentThread().setContextClassLoader( originalClassLoader );
    }
    return null;
  }

  @Override
  public String getHadoopVersion() {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      return VersionInfo.getVersion();
    } finally {
      Thread.currentThread().setContextClassLoader( originalClassLoader );
    }
  }

  @Override
  public Driver getHiveJdbcDriver() {
    return null;
  }

  protected void validateHadoopHomeWithWinutils() {
    try {
      ShellPrevalidator.doesWinutilsFileExist();
    } catch ( IOException e ) {
      logger.error( BaseMessages.getString( CommonHadoopShim.class,
        "CommonHadoopShim.HadoopHomeNotSet" ), e );
    }
  }

  @Override
  public Driver getJdbcDriver( String driverType ) {
    try {
      Driver newInstance = null;
      Class<? extends Driver> clazz = JDBC_DRIVER_MAP.get( driverType );
      if ( clazz != null ) {
        newInstance = clazz.newInstance();
        return DriverProxyInvocationChain.getProxy( Driver.class, newInstance );
      } else {
        clazz = tryToLoadDriver( JDBC_POSSIBLE_DRIVER_MAP.get( driverType ) );
        if ( clazz != null ) {
          newInstance = clazz.newInstance();
          if ( driverType.equals( "Impala" ) ) {
            return DriverProxyInvocationChain.getProxy( Driver.class, newInstance );
          }
          return newInstance;
        }
        return null;
      }

    } catch ( Exception ex ) {
      throw new ShimRuntimeException( "Unable to load JDBC driver of type: " + driverType, ex );
    }
  }

  @Override
  public Configuration createConfiguration() {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new org.pentaho.hadoop.shim.common.ConfigurationProxy();
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public Configuration createConfiguration( String namedCluster ) {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new org.pentaho.hadoop.shim.common.ConfigurationProxy( namedCluster );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public FileSystem getFileSystem( Configuration conf ) throws IOException {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    conf.set( FS_HDFS_IMPL,
      org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    conf.set( FS_FILE_IMPL,
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
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
    conf.set( FS_HDFS_IMPL,
      org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    conf.set( FS_FILE_IMPL,
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    try ( FileSystemProxy fsp = new FileSystemProxy(
      org.apache.hadoop.fs.FileSystem.get( uri, ShimUtils.asConfiguration( conf ), user ) ) ) {
      return fsp;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override public FileSystem getFileSystem( URI uri, Configuration conf, NamedCluster namedCluster )
    throws IOException, InterruptedException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    conf.set( FS_HDFS_IMPL,
      org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    conf.set( FS_FILE_IMPL,
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    try ( FileSystemProxy fsp = new FileSystemProxy(
      org.apache.hadoop.fs.FileSystem.get( uri, ShimUtils.asConfiguration( conf ) ) ) ) {
      return fsp;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  public void setDistributedCacheUtil( DistributedCacheUtil dcUtilParam ) {
    if ( dcUtilParam == null ) {
      throw new NullPointerException();
    }
    this.dcUtil = dcUtilParam;
  }

  @Override
  public DistributedCacheUtil getDistributedCacheUtil() throws ConfigurationException {
    if ( dcUtil == null ) {
      dcUtil = new DistributedCacheUtilImpl();
    }
    return dcUtil;
  }

  @Override
  public String[] getNamenodeConnectionInfo( Configuration c ) {
    URI namenode = org.apache.hadoop.fs.FileSystem.getDefaultUri( ShimUtils.asConfiguration( c ) );
    String[] result = new String[ 2 ];
    if ( namenode != null ) {
      result[ 0 ] = namenode.getHost();
      if ( namenode.getPort() != -1 ) {
        result[ 1 ] = String.valueOf( namenode.getPort() );
      }
    }
    return result;
  }

  @Override
  public String[] getJobtrackerConnectionInfo( Configuration c ) {
    String[] result = new String[ 2 ];
    if ( !"local".equals( c.get( MAPRED_JOB_TRACKER, "local" ) ) ) {
      InetSocketAddress jobtracker = getJobTrackerAddress( c );
      result[ 0 ] = jobtracker.getHostName();
      result[ 1 ] = String.valueOf( jobtracker.getPort() );
    }
    return result;
  }

  public static InetSocketAddress getJobTrackerAddress( Configuration conf ) {
    String jobTrackerStr = conf.get( MAPRED_JOB_TRACKER, "localhost:8012" );
    return NetUtils.createSocketAddr( jobTrackerStr );
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                              String jobtrackerPort, Configuration conf, List<String> logMessages )
    throws Exception {

    if ( namenodeHost == null || namenodeHost.trim().length() == 0 ) {
      throw new ConfigurationException( "No hdfs host specified!" );
    }
    if ( jobtrackerHost == null || jobtrackerHost.trim().length() == 0 ) {
      throw new ConfigurationException( "No job tracker host specified!" );
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

    if ( jobtrackerPort == null || jobtrackerPort.trim().length() == 0 ) {
      jobtrackerPort = getDefaultJobtrackerPort();
      logMessages.add( "No job tracker port specified - using default: " + jobtrackerPort );
    }

    String fsDefaultName = "hdfs://" + namenodeHost + namenodePort;
    String jobTracker = jobtrackerHost + ":" + jobtrackerPort;

    conf.set( "fs.default.name", fsDefaultName );
    conf.set( MAPRED_JOB_TRACKER, jobTracker );
  }

  /**
   * @return the default port of the namenode
   */
  protected String getDefaultNamenodePort() {
    return DEFAULT_NAMENODE_PORT;
  }

  /**
   * @return the default port of the jobtracker
   */
  protected String getDefaultJobtrackerPort() {
    return DEFAULT_JOBTRACKER_PORT;
  }

  @Override
  public RunningJob submitJob( Configuration c ) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return c.submit();
    } catch ( ClassNotFoundException e ) {
      throw new ShimRuntimeException( "Caught ClassNotFoundException.", e );
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      throw new ShimRuntimeException( "Caught InterruptedException.", e );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[ 0 ];
  }

  @Override
  public Class<? extends Writable> getHadoopWritableCompatibleClass( ValueMetaInterface kettleType ) {
    if ( kettleType == null ) {
      return NullWritable.class;
    }
    switch ( kettleType.getType() ) {
      case ValueMetaInterface.TYPE_STRING:
      case ValueMetaInterface.TYPE_BIGNUMBER:
      case ValueMetaInterface.TYPE_DATE:
        return Text.class;
      case ValueMetaInterface.TYPE_INTEGER:
        return LongWritable.class;
      case ValueMetaInterface.TYPE_NUMBER:
        return DoubleWritable.class;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return BooleanWritable.class;
      case ValueMetaInterface.TYPE_BINARY:
        return BytesWritable.class;
      default:
        return Text.class;
    }
  }

  @Override
  public String getPentahoMapReduceCombinerClass() {
    return PENTAHO_MAPREDUCE_GENERIC_COMBINER_CLASS_NAME;
  }

  @Override
  public String getPentahoMapReduceReducerClass() {
    return PENTAHO_MAPREDUCE_GENERIC_REDUCER_CLASS_NAME;
  }

  @Override
  public String getPentahoMapReduceMapRunnerClass() {
    return PENTAHO_MAPREDUCE_RUNNABLE_CLASS_NAME;
  }
}
