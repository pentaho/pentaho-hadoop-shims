/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.hadoop.hive.jdbc.HiveDriver;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.GenericTransCombiner;
import org.pentaho.hadoop.mapreduce.GenericTransReduce;
import org.pentaho.hadoop.mapreduce.PentahoMapRunnable;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hdfs.vfs.HDFSFileProvider;

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
  private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger( getClass() );

  public static class NotSupportedDriver implements Driver {
    public static SQLException notSupported =
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
  protected static Map<String, Class<? extends Driver>> JDBC_DRIVER_MAP =
    new HashMap<String, Class<? extends Driver>>() {
      {
        put( "hive", org.apache.hadoop.hive.jdbc.HiveDriver.class );
      }
    };

  @SuppressWarnings( "serial" )
  protected static Map<String, String> JDBC_POSSIBLE_DRIVER_MAP =
    new HashMap<String, String>();

  static {
    JDBC_POSSIBLE_DRIVER_MAP.put( "hive2Simba", "org.pentaho.hadoop.shim.common.CommonHadoopShim$NotSupportedDriver" );
    JDBC_POSSIBLE_DRIVER_MAP.put( "ImpalaSimba", "org.pentaho.hadoop.shim.common.CommonHadoopShim$NotSupportedDriver" );
    JDBC_POSSIBLE_DRIVER_MAP
      .put( "SparkSqlSimba", "org.pentaho.hadoop.shim.common.CommonHadoopShim$NotSupportedDriver" );
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
      e2.printStackTrace();
    } finally {
      Thread.currentThread().setContextClassLoader( originalClassLoader );
    }
    return null;
  }

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  @Override
  public String getHadoopVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    validateHadoopHomeWithWinutils();
    fsm.addProvider( config, "hdfs", config.getIdentifier(), new HDFSFileProvider() );
    setDistributedCacheUtil( new DistributedCacheUtilImpl( config ) );
  }

  @Override
  public Driver getHiveJdbcDriver() {
    try {
      return new HiveDriver();
    } catch ( Exception ex ) {
      throw new RuntimeException( "Unable to load Hive JDBC driver", ex );
    }
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
      throw new RuntimeException( "Unable to load JDBC driver of type: " + driverType, ex );
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
  public FileSystem getFileSystem( Configuration conf ) throws IOException {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new FileSystemProxy( org.apache.hadoop.fs.FileSystem.get( ShimUtils.asConfiguration( conf ) ) );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public FileSystem getFileSystem( URI uri, Configuration conf, String user ) throws IOException, InterruptedException {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new FileSystemProxy( org.apache.hadoop.fs.FileSystem.get( uri, ShimUtils.asConfiguration( conf ), user ) );
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
      throw new ConfigurationException( BaseMessages.getString( CommonHadoopShim.class,
        "CommonHadoopShim.DistributedCacheUtilMissing" ) );
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
    if ( !"local".equals( c.get( "mapred.job.tracker", "local" ) ) ) {
      InetSocketAddress jobtracker = getJobTrackerAddress( c );
      result[ 0 ] = jobtracker.getHostName();
      result[ 1 ] = String.valueOf( jobtracker.getPort() );
    }
    return result;
  }

  public static InetSocketAddress getJobTrackerAddress( Configuration conf ) {
    String jobTrackerStr = conf.get( "mapred.job.tracker", "localhost:8012" );
    return NetUtils.createSocketAddr( jobTrackerStr );
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                              String jobtrackerPort, Configuration conf, List<String> logMessages )
    throws Exception {

    if ( namenodeHost == null || namenodeHost.trim().length() == 0 ) {
      throw new Exception( "No hdfs host specified!" );
    }
    if ( jobtrackerHost == null || jobtrackerHost.trim().length() == 0 ) {
      throw new Exception( "No job tracker host specified!" );
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
    conf.set( "mapred.job.tracker", jobTracker );
  }

  /**
   * @return the default port of the namenode
   */
  protected String getDefaultNamenodePort() {
    return "9000";
  }

  /**
   * @return the default port of the jobtracker
   */
  protected String getDefaultJobtrackerPort() {
    return "9001";
  }

  @Override
  public RunningJob submitJob( Configuration c ) throws IOException {
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
  public Class<? extends Writable> getHadoopWritableCompatibleClass( ValueMetaInterface kettleType ) {
    return TypeConverterFactory.getWritableForKettleType( kettleType );
  }

  @Override
  public Class<?> getPentahoMapReduceCombinerClass() {
    return GenericTransCombiner.class;
  }

  @Override
  public Class<?> getPentahoMapReduceReducerClass() {
    return GenericTransReduce.class;
  }

  @Override
  public Class<?> getPentahoMapReduceMapRunnerClass() {
    return PentahoMapRunnable.class;
  }
}
