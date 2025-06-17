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

import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by bryan on 4/18/16.
 */
public class DriverLocatorImpl implements DriverLocator {
  private static final Logger logger = LoggerFactory.getLogger( DriverLocatorImpl.class );
  public static final String DATA_SOURCE_TYPE_BIGDATA = "(dataSourceType=bigdata)";
  private final HasRegisterDriver hasRegisterDriver;
  private final HasDeregisterDriver hasDeregisterDriver;
  private final List<Driver> registeredDrivers;
  private static DriverLocatorImpl instance;

  public static DriverLocatorImpl getInstance() {
    if ( instance == null ) {
      instance = new DriverLocatorImpl();
    }
    return instance;
  }

  public DriverLocatorImpl() {
    this( DriverManager::registerDriver, DriverManager::deregisterDriver, new ArrayList<>() );
  }

  public DriverLocatorImpl( HasRegisterDriver hasRegisterDriver,
                            HasDeregisterDriver hasDeregisterDriver,
                            List<Driver> registeredDrivers ) {
    this.hasRegisterDriver = hasRegisterDriver;
    this.hasDeregisterDriver = hasDeregisterDriver;
    this.registeredDrivers = registeredDrivers;
  }

  public void registerDriver( Driver driver ){
    registeredDrivers.add( driver );
  }

  public Iterator<Driver> getDrivers() {
    return registeredDrivers.iterator();
  }

  @Override public Driver getDriver( String url ) {
    Iterator<Driver> drivers = getDrivers();
    while ( drivers.hasNext() ) {
      Driver driver = drivers.next();
      try {
        if ( driver.acceptsURL( url ) ) {
          return driver;
        }
      } catch ( SQLException e ) {
        logger.error( String.format( "Unable to see if driver %s acceptsURL %s", driver, url ) );
      }
    }
    return null;
  }

  public synchronized void registerDriverServiceReferencePair( Driver driver,
                                                               boolean shouldRegisterExternally ) {
    try {
      // this registerDriverServiceReferencePair method is currently only called in LazyDelegatingDriver.findAndProcess
      // and shouldRegisterExternally is always false... so lets assume we don't have the responsibility of deregister it
      if ( shouldRegisterExternally ) {
        hasRegisterDriver.registerDriver( driver );
      }
      registeredDrivers.add( driver );
    } catch ( SQLException e ) {
      logger.error( "Unable to register driver " + driver, e );
    }
  }
}
