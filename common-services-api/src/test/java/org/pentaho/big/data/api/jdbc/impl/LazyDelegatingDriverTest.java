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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.database.DelegatingDriver;

import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith ( MockitoJUnitRunner.class )
public class LazyDelegatingDriverTest {


  @Test
  public void testDestroyDelegatingDriverDeresgistered() throws SQLException {
    HasRegisterDriver registerDriverFunction = mock( HasRegisterDriver.class );
    HasDeregisterDriver deregisterDriverFunction = mock( HasDeregisterDriver.class );
    DriverLocatorImpl driverLocator = mock( DriverLocatorImpl.class );

    LazyDelegatingDriver driver = new LazyDelegatingDriver( driverLocator, registerDriverFunction, deregisterDriverFunction );

    // delegating driver was registered
    DelegatingDriver registeredDelegatingDriver = driver.delegatingDriver;
    assertNotNull( driver.delegatingDriver );

    // act
    driver.destroy();

    // assert
    verify( deregisterDriverFunction, times( 1 ) ).deregisterDriver( ArgumentMatchers.eq( registeredDelegatingDriver )  );
    assertNull( driver.delegatingDriver );

  }

  @Test
  public void testDestroyLazyDelegatingDriverDeresgistered() throws SQLException {
    HasRegisterDriver registerDriverFunction = mock( HasRegisterDriver.class );
    HasDeregisterDriver deregisterDriverFunction = mock( HasDeregisterDriver.class );
    DriverLocatorImpl driverLocator = mock( DriverLocatorImpl.class );

    LazyDelegatingDriver driver = new LazyDelegatingDriver( driverLocator, registerDriverFunction, deregisterDriverFunction );

    LazyDelegatingDriver lazyDelegatingDriver = mock( LazyDelegatingDriver.class );
    driver.lazyDelegatingDriver = lazyDelegatingDriver;

    // act
    driver.destroy();

    // assert
    verify( lazyDelegatingDriver, times( 1 ) ).destroy();
    assertNull( driver.delegatingDriver );
  }

}
