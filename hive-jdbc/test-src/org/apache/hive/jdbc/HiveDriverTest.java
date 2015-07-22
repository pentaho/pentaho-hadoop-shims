/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.apache.hive.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.pentaho.hadoop.hive.jdbc.HadoopConfigurationUtil;
import org.pentaho.hadoop.hive.jdbc.JDBCDriverCallable;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

public class HiveDriverTest {

  protected String SCHEME_STRING = "hive2";
  protected String URL_SUITABLE = "jdbc.hive2://host:port/";
  protected String URL_UNSUITABLE = "jdbc.hive2://host:port/AuthMech=0";

  protected HadoopShim getMockShimWithDriver( final Driver driver ) {
    return new MockHadoopShim() {
      @Override
      public Driver getJdbcDriver( String scheme ) {
        return scheme.equalsIgnoreCase( SCHEME_STRING ) ? driver : null;
      }
    };
  }

  protected HadoopConfigurationUtil getMockUtil( final HadoopShim shim ) {
    return new HadoopConfigurationUtil() {
      @Override
      public Object getActiveHadoopShim() throws Exception {
        return shim;
      }
    };
  }

  @Test( expected = NullPointerException.class )
  public void instantiation_no_util() {
    getActiveTestDriverInstance( (HadoopConfigurationUtil) null );
  }

  @Test
  public void getActiveDriver() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    HadoopShim shim = new MockHadoopShim() {

      public java.sql.Driver getJdbcDriver( String scheme ) {
        if ( scheme.equalsIgnoreCase( SCHEME_STRING ) ) {
          called.set( true );
          return new MockDriver();
        } else {
          return null;
        }
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );
    ( (HiveDriver) d ).getActiveDriver();
    assertTrue( "Shim's getJdbcDriver(\"hive2\") not called", called.get() );
  }

  @Test
  public void getActiveDriver_exception_in_getJdbcDriver() {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        if ( scheme.equalsIgnoreCase( SCHEME_STRING ) ) {
          throw new RuntimeException();
        } else {
          return null;
        }
      }
    };

    try {
      Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );
      ( (HiveDriver) d ).getActiveDriver();
      fail( "Expected exception" );
    } catch ( SQLException ex ) {
      assertEquals( InvocationTargetException.class, ex.getCause().getClass() );
      assertEquals( "Unable to load Hive Server 2 JDBC driver for the currently active Hadoop configuration",
        ex.getMessage() );
    }
  }

  @Test
  public void getActiveDriver_null_in_getJdbcDriver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );

    assertNull( ( (HiveDriver) d ).getActiveDriver() );
  }

  @Test
  public void getActiveDriver_same_driver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        // Return another shim driver. This should fail when called since the
        // classes are the same
        return ( scheme.equalsIgnoreCase( SCHEME_STRING ) ) ? getActiveTestDriverInstance() : null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );

    assertNull( ( (HiveDriver) d ).getActiveDriver() );

  }

  @Test( expected = SQLException.class )
  public void callWithActiveDriver_same_driver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        // Return another shim driver. This should fail when called since the
        // classes are the same
        return ( scheme.equalsIgnoreCase( SCHEME_STRING ) ) ? getActiveTestDriverInstance() : null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );

    ( (HiveDriver) d ).callWithActiveDriver( new JDBCDriverCallable<String>() {
      @Override
      public String call() throws Exception {
        return "test";
      }
    } );
  }

  @Test( expected = SQLException.class )
  public void callWithActiveDriver_null_driver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        // Return another shim driver. This should fail when called since the
        // classes are the same
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );

    ( (HiveDriver) d ).callWithActiveDriver( new JDBCDriverCallable<String>() {
      @Override
      public String call() throws Exception {
        return "test";
      }
    } );
  }

  @Test
  public void connect() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public Connection connect( String url, Properties info ) throws SQLException {
        called.set( true );
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.connect( URL_SUITABLE, new Properties() );
    assertTrue( called.get() );
  }

  @Test
  public void connect_returns_null_if_shim_doesnt_have_hive2_driver() {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );

    try {
      assertThat( d.connect( URL_SUITABLE, new Properties() ), is( nullValue() ) );
    } catch ( SQLException ex ) {
      fail( "Should not get exception if there is no hive2 driver in shim" );
    }
  }

  @Test
  public void acceptsURL() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public boolean acceptsURL( String url ) throws SQLException {
        called.set( true );
        return false;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.acceptsURL( URL_SUITABLE );
    assertTrue( called.get() );
  }

  @Test
  public void acceptsURL_no_driver() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    HadoopShim shim = new MockHadoopShim() {
      @Override
      public Driver getJdbcDriver( String scheme ) {
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( shim ) );

    assertFalse( d.acceptsURL( URL_UNSUITABLE ) );

  }

  @Test
  public void getPropertyInfo() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
        called.set( true );
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.getPropertyInfo( URL_SUITABLE, new Properties() );
    assertTrue( called.get() );
  }

  @Test
  public void getMajorVersion() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public int getMajorVersion() {
        called.set( true );
        return 0;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.getMajorVersion();
    assertTrue( called.get() );
  }

  @Test
  public void getMajorVersion_exception() {
    Driver driver = new MockDriver() {
      @Override
      public int getMajorVersion() {
        throw new NullPointerException();
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    // If an exception is thrown the version returned should be -1
    assertEquals( -1, d.getMajorVersion() );
  }

  @Test
  public void getMinorVersion() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public int getMinorVersion() {
        called.set( true );
        return 0;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.getMinorVersion();
    assertTrue( called.get() );
  }

  @Test
  public void getMinorVersion_exception() {
    Driver driver = new MockDriver() {
      @Override
      public int getMinorVersion() {
        throw new NullPointerException();
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    // If an exception is thrown the version returned should be -1
    assertEquals( -1, d.getMinorVersion() );
  }

  protected Driver getActiveTestDriverInstance( Driver driver ) {
    return getActiveTestDriverInstance( getMockUtil( getMockShimWithDriver( driver ) ) );
  }

  protected Driver getActiveTestDriverInstance( HadoopConfigurationUtil util ) {
    return new HiveDriver( util );
  }

  protected Driver getActiveTestDriverInstance() {
    return new HiveDriver();
  }

  @Test
  public void jdbcCompliant() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public boolean jdbcCompliant() {
        called.set( true );
        return false;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.jdbcCompliant();
    assertTrue( called.get() );
  }

  @Test
  public void jdbcCompliant_exception() throws SQLException {
    Driver driver = new MockDriver() {
      @Override
      public boolean jdbcCompliant() {
        throw new NullPointerException();
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    // should return false if there is an exception
    assertFalse( d.jdbcCompliant() );
  }

  @Test
  public void connectSimbaUrl() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public Connection connect( String url, Properties info ) throws SQLException {
        called.set( true );
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( driver );

    d.connect( URL_UNSUITABLE, new Properties() );
    assertFalse( called.get() );
  }

}
