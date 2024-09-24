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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;
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
    verify( deregisterDriverFunction, times( 1 ) ).deregisterDriver( Matchers.eq( registeredDelegatingDriver )  );
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
