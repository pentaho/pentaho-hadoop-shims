/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
