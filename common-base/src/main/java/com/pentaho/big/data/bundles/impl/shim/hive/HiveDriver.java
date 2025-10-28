/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package com.pentaho.big.data.bundles.impl.shim.hive;

import org.pentaho.big.data.api.jdbc.impl.JdbcUrlImpl;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrl;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;
import org.pentaho.hadoop.shim.common.DriverProxyInvocationChain;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by bryan on 3/29/16.
 */
public class HiveDriver implements Driver {
  protected static final String SIMBA_SPECIFIC_URL_PARAMETER = "AuthMech=";
  /**
   * SQL State "feature not supported" with no subclass specified
   */
  public static final String SQL_STATE_NOT_SUPPORTED = "0A000";
  protected final Driver delegate;
  private final boolean defaultConfiguration;
  protected final JdbcUrlParser jdbcUrlParser;
  protected final String hadoopConfigurationId;

  public HiveDriver( JdbcUrlParser jdbcUrlParser,
                     String className, String shimVersion )
    throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    this( jdbcUrlParser, className, shimVersion, "hive2" );
  }

  public HiveDriver( JdbcUrlParser jdbcUrlParser,
                     String className, String shimVersion, String driverType )
    throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Driver driverClass = null;
    boolean driverFound = false;
    try {
      driverClass = (Driver) Class.forName( className ).newInstance();
      driverFound = true;
    } catch ( ClassNotFoundException e ) {
      // allow class to initialize but prevent driver from actually being called
      driverClass = (Driver) Class.forName( "org.apache.hive.jdbc.HiveDriver" ).newInstance();
    }
    this.hadoopConfigurationId = driverFound ? shimVersion : null;
    this.delegate = DriverProxyInvocationChain.getProxy( Driver.class, driverClass );
    this.defaultConfiguration = true;
    this.jdbcUrlParser = jdbcUrlParser;
  }

  public HiveDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                     JdbcUrlParser jdbcUrlParser ) {
    this.delegate = delegate;
    this.hadoopConfigurationId = hadoopConfigurationId;
    this.defaultConfiguration = defaultConfiguration;
    this.jdbcUrlParser = jdbcUrlParser;
  }

  @Override public Connection connect( String url, Properties info ) throws SQLException {
    if ( !checkBeforeAccepting( url ) ) {
      return null;
    }
    Driver driver = checkBeforeCallActiveDriver( url );
    JdbcUrl jdbcUrl;
    try {
      jdbcUrl = jdbcUrlParser.parse( url );
    } catch ( URISyntaxException e1 ) {
      throw new SQLException( "Unable to parse jdbc url: " + url, e1 );
    }
    NamedCluster namedCluster;
    try {
      namedCluster = jdbcUrl.getNamedCluster();
    } catch ( Exception e ) {
      return null;
    }
    if ( !acceptsURL( url, driver, namedCluster ) ) {
      return null;
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      Connection hiveConn = doConnect( driver, jdbcUrl, info );
      return hiveConn;
    } catch ( Exception ex ) {
      Throwable cause = ex;
      do {
        // BACKLOG-6547
        if ( cause instanceof SQLException
          && SQL_STATE_NOT_SUPPORTED.equals( ( (SQLException) cause ).getSQLState() ) ) {
          // this means that either driver can't be obtained or does not support connect().
          // In both cases signal to DriverManager we can't process the URL
          return null;
        }
        cause = cause.getCause();
      } while ( cause != null );

      throw ex;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  public Connection doConnect( Driver driver, JdbcUrl url, Properties info ) throws SQLException {
    return driver.connect( url.toString().replaceFirst( JdbcUrlImpl.PENTAHO_NAMED_CLUSTER + "=[^;]*;", "" ), info );
  }

  @Override public final boolean acceptsURL( String url ) {
    try {
      JdbcUrl jdbcUrl = jdbcUrlParser.parse( url );
      NamedCluster namedCluster = jdbcUrl.getNamedCluster();
      return acceptsURL( url, checkBeforeCallActiveDriver( url ), namedCluster );
    } catch ( Exception e ) {
      return false;
    }
  }

  private final boolean acceptsURL( String url, Driver driver, NamedCluster namedCluster ) throws SQLException {

    if ( !defaultConfiguration ) {
      return false;
    }

    if ( driver == null ) {
      return false;
    }
    try {
      return isRequiredShim( namedCluster, url ) && driver.acceptsURL( url );
    } catch ( Throwable e ) {
      // This should not have happened. If there was an error during processing, assume this driver can't
      // handle the URL and thus return false
      return false;
    }
  }

  protected boolean isRequiredShim( NamedCluster namedCluster, String url ) {
    // Either hadoopConfigurationId matches the namedCluster shim ID, or the namedCluster shim ID is null and
    // hadoopConfigurationId matches the shim identified in the legacy properties file (legacy configuration support)
    boolean useThisShim = false;
    if ( namedCluster != null && namedCluster.getShimIdentifier() != null ) {
      useThisShim = hadoopConfigurationId != null && hadoopConfigurationId.equals( namedCluster.getShimIdentifier() );
    } else {
      useThisShim = hadoopConfigurationId.equals( getLegacyDefaultShimName() );
    }
    return useThisShim;
  }

  private String getLegacyDefaultShimName() {
    String defaultShimName = null;
    try {
      defaultShimName = LegacyShimLocator.getLegacyDefaultShimName();
    } catch ( IOException e ) {
      // do nothing; fallback failed
    }
    return defaultShimName;
  }

  protected Driver checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) || !checkBeforeAccepting( url ) ) {
      // BAD-215 check required to distinguish Simba driver
      return null;
    }
    return delegate;
  }

  protected boolean checkBeforeAccepting( String url ) {
    return ( hadoopConfigurationId != null ) && url.matches( ".+:hive2:.*" );
  }

  @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    Driver driverDelegate = this.delegate;
    if ( driverDelegate == null ) {
      return null;
    }
    return driverDelegate.getPropertyInfo( url, info );
  }

  @Override public int getMajorVersion() {
    Driver driverDelegate = this.delegate;
    if ( driverDelegate == null ) {
      return -1;
    }
    return driverDelegate.getMajorVersion();
  }

  @Override public int getMinorVersion() {
    Driver driverDelegate = this.delegate;
    if ( driverDelegate == null ) {
      return -1;
    }
    return driverDelegate.getMinorVersion();
  }

  @Override public boolean jdbcCompliant() {
    Driver driverDelegate = this.delegate;
    if ( driverDelegate == null ) {
      return false;
    }
    try {
      return driverDelegate.jdbcCompliant();
    } catch ( Throwable e ) {
      return false;
    }
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    Driver driverDelegate = this.delegate;
    if ( driverDelegate == null ) {
      return null;
    }
    try {
      return driverDelegate.getParentLogger();
    } catch ( Throwable e ) {
      if ( e instanceof SQLFeatureNotSupportedException ) {
        throw e;
      } else {
        throw new SQLFeatureNotSupportedException( e );
      }
    }
  }
}
