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


package org.pentaho.big.data.api.jdbc.impl;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.database.DelegatingDriver;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by bryan on 4/27/16.
 */
public class ClusterInitializingDriver implements Driver {

  private static final List<String> BIG_DATA_DRIVER_URL_PATTERNS = new ArrayList<>();

  @VisibleForTesting
  protected static org.slf4j.Logger logger = LoggerFactory.getLogger( ClusterInitializingDriver.class );

  private final JdbcUrlParser jdbcUrlParser;

  private final HasRegisterDriver hasRegisterDriver;
  private final HasDeregisterDriver hasDeregisterDriver;

  DelegatingDriver delegatingDriver;
  LazyDelegatingDriver[] lazyDelegatingDrivers;

  static {
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:hive:.*" );
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:hive2:.*" );
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:impala:.*" );
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:spark:.*" );
  }

  public ClusterInitializingDriver( JdbcUrlParser jdbcUrlParser,
                                    DriverLocatorImpl driverRegistry ) {
    this( jdbcUrlParser, driverRegistry, null );
  }

  // Called by Blueprint
  public ClusterInitializingDriver( JdbcUrlParser jdbcUrlParser,
                                    DriverLocatorImpl driverRegistry, Integer numLazyProxies ) {
    this( jdbcUrlParser, driverRegistry, numLazyProxies, DriverManager::registerDriver,
      DriverManager::deregisterDriver );
  }

  public ClusterInitializingDriver( JdbcUrlParser jdbcUrlParser,
                                    DriverLocatorImpl driverRegistry, Integer numLazyProxies,
                                    HasRegisterDriver hasRegisterDriver, HasDeregisterDriver hasDeregisterDriver ) {
    this.jdbcUrlParser = jdbcUrlParser;

    this.hasRegisterDriver = hasRegisterDriver;
    this.hasDeregisterDriver = hasDeregisterDriver;

    int lazyProxies = Optional.ofNullable( numLazyProxies ).orElse( 5 );
    try {
      delegatingDriver = new DelegatingDriver( this );
      hasRegisterDriver.registerDriver( delegatingDriver );
    } catch ( SQLException e ) {
      logger.warn( "Unable to register cluster initializing driver", e );
    }

    lazyDelegatingDrivers = new LazyDelegatingDriver[lazyProxies];
    for ( int i = 0; i < lazyProxies; i++ ) {
      try {
        lazyDelegatingDrivers[i] = new LazyDelegatingDriver( driverRegistry, hasRegisterDriver, hasDeregisterDriver );
      } catch ( SQLException e ) {
        logger.warn( "Failed to register " + LazyDelegatingDriver.class.getName(), e );
      }
    }
  }

  // Called by Blueprint
  public void destroy() {
    try {
      if ( delegatingDriver != null ) {
        hasDeregisterDriver.deregisterDriver( delegatingDriver );
      }
    } catch ( SQLException e ) {
      logger.warn( "Unable to deregister cluster initializing driver", e );
    } finally {
      delegatingDriver = null;
    }

    for ( int i = 0; i != lazyDelegatingDrivers.length; ++i ) {
      if ( lazyDelegatingDrivers[i] != null ) {
        lazyDelegatingDrivers[i].destroy();
        lazyDelegatingDrivers[i] = null;
      }
    }

    lazyDelegatingDrivers = null;
  }

  @Override
  public Connection connect( String url, Properties info ) throws SQLException {
    if ( checkIfUsesBigDataDriver( url ) ) {
      initializeCluster( url );
    }
    return null;
  }

  @Override
  public boolean acceptsURL( String url ) throws SQLException {
    if ( checkIfUsesBigDataDriver( url ) ) {
      initializeCluster( url );
    }
    return false;
  }

  boolean checkIfUsesBigDataDriver( String url ) {
    List<String> urlPatterns = getUrlPatternsForBigDataDrivers();
    for ( String pattern : urlPatterns ) {
      if ( url.matches( pattern ) ) {
        return true;
      }
    }
    return false;
  }

  List<String> getUrlPatternsForBigDataDrivers() {
    return BIG_DATA_DRIVER_URL_PATTERNS;
  }

  private void initializeCluster( String url ) {
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return new DriverPropertyInfo[ 0 ];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
