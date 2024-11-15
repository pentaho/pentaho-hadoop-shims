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

import org.osgi.framework.ServiceReference;
import org.pentaho.di.core.database.DelegatingDriver;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * Created by bryan on 4/27/16.
 */
public class LazyDelegatingDriver implements Driver {
  private final DriverLocatorImpl driverLocator;
  private final HasRegisterDriver hasRegisterDriver;
  private final HasDeregisterDriver hasDeregisterDriver;
  private Driver delegate;
  DelegatingDriver delegatingDriver;
  LazyDelegatingDriver lazyDelegatingDriver;

  protected static org.slf4j.Logger logger = LoggerFactory.getLogger( LazyDelegatingDriver.class );

  public LazyDelegatingDriver( DriverLocatorImpl driverLocator ) throws SQLException {
    this( driverLocator, DriverManager::registerDriver, DriverManager::deregisterDriver );
  }

  public LazyDelegatingDriver( DriverLocatorImpl driverLocator,
                               HasRegisterDriver hasRegisterDriver, HasDeregisterDriver hasDeregisterDriver )
    throws SQLException {
    this.driverLocator = driverLocator;
    this.hasRegisterDriver = hasRegisterDriver;
    this.hasDeregisterDriver = hasDeregisterDriver;
    this.delegatingDriver = new DelegatingDriver( this );
    hasRegisterDriver.registerDriver( delegatingDriver );
  }

  public void destroy() {
    try {
      // it's our responsibility to deregister delegatingDriver: we're the ones that registered it (in the constructor
      // above) and driverLocator is instructed not to do so (in findAndProcess bellow)
      // not sure if it can already be deregistered by the DriverLocatorImpl's Service Listener, but it doesn't hurt
      // trying
      if ( delegatingDriver != null ) {
        hasDeregisterDriver.deregisterDriver( delegatingDriver );
      }
    } catch ( SQLException e ) {
      logger.warn( "Failed to deregister " + LazyDelegatingDriver.class.getName(), e );
    } finally {
      delegatingDriver = null;
    }

    if ( lazyDelegatingDriver != null ) {
      lazyDelegatingDriver.destroy();
      lazyDelegatingDriver = null;
    }
  }

  private synchronized <T> T findAndProcess( FunctionWithSQLException<Driver, T> attempt, Predicate<T> success,
                                             T defaultVal )
    throws SQLException {
    if ( delegate == null ) {
      Iterator<Map.Entry<ServiceReference<Driver>, Driver>> drivers = driverLocator.getDrivers();
      while ( drivers.hasNext() ) {
        Map.Entry<ServiceReference<Driver>, Driver> driverEntry = drivers.next();
        ServiceReference<Driver> serviceReference = driverEntry.getKey();
        Driver driver = driverEntry.getValue();
        T result = attempt.apply( driver );
        if ( success.test( result ) ) {
          delegate = driver;

          // why do we need this LazyDelegatingDriver? keeping reference to deregister it later
          lazyDelegatingDriver = new LazyDelegatingDriver( driverLocator, hasRegisterDriver, hasDeregisterDriver );

          driverLocator.registerDriverServiceReferencePair( serviceReference, delegatingDriver, false );
          return result;
        }
      }
    } else {
      T result = attempt.apply( delegate );
      if ( success.test( result ) ) {
        return result;
      }
    }
    return defaultVal;
  }

  private synchronized <T> T process( Function<Driver, T> function, T defaultVal ) {
    if ( delegate == null ) {
      return defaultVal;
    }
    return function.apply( delegate );
  }

  private synchronized <T> T processSQLException( FunctionWithSQLException<Driver, T> function, T defaultVal )
    throws SQLException {
    if ( delegate == null ) {
      return defaultVal;
    }
    return function.apply( delegate );
  }

  private synchronized <T> T processSQLFeatureNotSupportedException(
    FunctionWithSQLFeatureNotSupportedException<Driver, T> function, T defaultVal )
    throws SQLFeatureNotSupportedException {
    if ( delegate == null ) {
      return defaultVal;
    }
    return function.apply( delegate );
  }

  @Override public Connection connect( String url, Properties info ) throws SQLException {
    return findAndProcess( driver -> driver.connect( url, info ), Objects::nonNull, null );
  }

  @Override public boolean acceptsURL( String url ) throws SQLException {
    return findAndProcess( driver -> driver.acceptsURL( url ), bool -> bool, false );
  }

  @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return processSQLException( driver -> driver.getPropertyInfo( url, info ), new DriverPropertyInfo[ 0 ] );
  }

  @Override public int getMajorVersion() {
    return process( driver -> driver.getMajorVersion(), 0 );
  }

  @Override public int getMinorVersion() {
    return process( driver -> driver.getMinorVersion(), 0 );
  }

  @Override public boolean jdbcCompliant() {
    return process( driver -> driver.jdbcCompliant(), false );
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return processSQLFeatureNotSupportedException( driver -> driver.getParentLogger(), null );
  }

  private interface FunctionWithSQLException<T, R> {
    R apply( T t ) throws SQLException;
  }

  private interface FunctionWithSQLFeatureNotSupportedException<T, R> {
    R apply( T t ) throws SQLFeatureNotSupportedException;
  }
}
