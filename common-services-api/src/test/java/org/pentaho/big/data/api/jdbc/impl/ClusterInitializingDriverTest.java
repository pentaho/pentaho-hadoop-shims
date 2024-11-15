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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.database.DelegatingDriver;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith ( MockitoJUnitRunner.class )
public class ClusterInitializingDriverTest {


  @Test
  public void testDestroyDelegatingDriverDeresgistered() throws SQLException {
    HasRegisterDriver registerDriverFunction = mock( HasRegisterDriver.class );
    HasDeregisterDriver deregisterDriverFunction = mock( HasDeregisterDriver.class );
    DriverLocatorImpl driverLocator = mock( DriverLocatorImpl.class );
    JdbcUrlParser jdbcUrlParser = mock( JdbcUrlParser.class );

    ClusterInitializingDriver driver = new ClusterInitializingDriver( jdbcUrlParser, driverLocator, 3,
      registerDriverFunction, deregisterDriverFunction );

    // delegating driver was registered
    DelegatingDriver registeredDelegatingDriver = driver.delegatingDriver;
    assertNotNull( driver.delegatingDriver );

    // act
    driver.destroy();

    // assert
    verify( deregisterDriverFunction, times( 1 ) )
      .deregisterDriver( Matchers.eq( registeredDelegatingDriver )  );
    assertNull( driver.delegatingDriver );

  }

  @Test
  public void testDestroyLazyDelegatingDriverDeregistered() throws SQLException {
    HasRegisterDriver registerDriverFunction = mock( HasRegisterDriver.class );
    HasDeregisterDriver deregisterDriverFunction = mock( HasDeregisterDriver.class );
    DriverLocatorImpl driverLocator = mock( DriverLocatorImpl.class );
    JdbcUrlParser jdbcUrlParser = mock( JdbcUrlParser.class );
    int numberOfLazyDelegatingDrivers = 3;

    ClusterInitializingDriver driver = new ClusterInitializingDriver(jdbcUrlParser, driverLocator,
      numberOfLazyDelegatingDrivers, registerDriverFunction, deregisterDriverFunction );

    LazyDelegatingDriver[] lazyDelegatingDrivers = driver.lazyDelegatingDrivers;
    assertNotNull( lazyDelegatingDrivers );
    assertEquals( numberOfLazyDelegatingDrivers, lazyDelegatingDrivers.length );

    LazyDelegatingDriver[] lazyDelegatingDriverSpies = Arrays.stream( lazyDelegatingDrivers )
      .map( Mockito::spy )
      .toArray( LazyDelegatingDriver[]::new );
    driver.lazyDelegatingDrivers = lazyDelegatingDriverSpies.clone(); // switch lazy delegating drivers with our spies.

    // act
    driver.destroy();

    // assert
    for ( LazyDelegatingDriver lazyDelegatingDriverSpy : lazyDelegatingDriverSpies ) {
      verify( lazyDelegatingDriverSpy, times( 1 ) ).destroy();
    }
    assertNull( driver.lazyDelegatingDrivers );
  }

}
