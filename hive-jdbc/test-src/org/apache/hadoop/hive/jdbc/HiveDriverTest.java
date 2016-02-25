/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

package org.apache.hadoop.hive.jdbc;

import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.junit.Test;
import org.pentaho.hadoop.hive.jdbc.HadoopConfigurationUtil;
import org.pentaho.hadoop.hive.jdbc.JDBCDriverCallable;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

public class HiveDriverTest {

  private HadoopShim getMockShimWithDriver( final Driver driver ) {
    return new MockHadoopShim() {
      @Override
      public Driver getJdbcDriver( String scheme ) {
        return scheme.equalsIgnoreCase( "hive" ) ? driver : null;
      }
    };
  }

  private HadoopShim getMockShimWithDriverClass( final Class<? extends Driver> driverClass ) {
    return new MockHadoopShim() {
      @Override
      public Driver getJdbcDriver( String scheme ) {
        try {
          return scheme.equalsIgnoreCase( "hive" ) ? driverClass.newInstance() : null;
        } catch ( Exception ex ) {
          throw new RuntimeException( "Wrapper", ex );
        }
      }
    };
  }

  private HadoopConfigurationUtil getMockUtil( final HadoopShim shim ) {
    return new HadoopConfigurationUtil() {
      @Override
      public Object getActiveHadoopShim() throws Exception {
        return shim;
      }
    };
  }

  @Test( expected = NullPointerException.class )
  public void instantiation_no_util() {
    new HiveDriver( null );
  }

  @Test
  public void getActiveDriver() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    HadoopShim shim = new MockHadoopShim() {

      public java.sql.Driver getJdbcDriver( String scheme ) {
        if ( scheme.equalsIgnoreCase( "hive" ) ) {
          called.set( true );
          return new MockDriver();
        } else {
          return null;
        }
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( shim ) );
    d.getActiveDriver();
    Assert.assertTrue( "Shim's getJdbcDriver(\"hive\") not called", called.get() );
  }

  @Test
  public void getActiveDriver_exception_in_getJdbcDriver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        if ( scheme.equalsIgnoreCase( "hive" ) ) {
          throw new RuntimeException();
        } else {
          return null;
        }
      }
    };

    try {
      HiveDriver d = new HiveDriver( getMockUtil( shim ) );
      d.getActiveDriver();
      Assert.fail( "Expected exception" );
    } catch ( SQLException ex ) {
      Assert.assertEquals( InvocationTargetException.class, ex.getCause().getClass() );
      Assert.assertEquals( "Unable to load Hive JDBC driver for the currently active Hadoop configuration", ex.getMessage() );
    }
  }

  @Test
  public void getActiveDriver_null_driver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        return null;
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( shim ) );

    Assert.assertNull( d.getActiveDriver() );
  }

  @Test
  public void getActiveDriver_same_driver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        // Return another shim driver. This should fail when called since the
        // classes are the same
        return ( scheme.equalsIgnoreCase( "hive" ) ) ? new HiveDriver() : null;
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( shim ) );

    Assert.assertNull( d.getActiveDriver() );
  }

  @Test( expected = SQLException.class )
  public void callWithActiveDriver_same_driver() throws Exception {
    HadoopShim shim = new MockHadoopShim() {
      public java.sql.Driver getJdbcDriver( String scheme ) {
        // Return another shim driver. This should fail when called since the
        // classes are the same
        return ( scheme.equalsIgnoreCase( "hive" ) ) ? new HiveDriver() : null;
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( shim ) );

    d.callWithActiveDriver( new JDBCDriverCallable<String>() {
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
    HiveDriver d = new HiveDriver( getMockUtil( shim ) );

    d.callWithActiveDriver( new JDBCDriverCallable<String>() {
      @Override
      public String call() throws Exception {
        return "test";
      }
    } );
  }
  @Test
  public void connect() throws SQLException {
    final AtomicBoolean connectCalled = new AtomicBoolean( false );
    final AtomicBoolean acceptsUrlCalled = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public Connection connect( String url, Properties info ) throws SQLException {
        connectCalled.set( true );
        return null;
      }

      @Override
      public boolean acceptsURL( String url ) throws SQLException {
        acceptsUrlCalled.set( true );
        return true;
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.connect( null, null );
    Assert.assertTrue( connectCalled.get() && acceptsUrlCalled.get() );
  }


  public static class CtorExceptionThrowingDriver implements Driver {

    public CtorExceptionThrowingDriver() throws SQLException {
      throw new SQLException( "Message", "0A000" ); // SQL State "feature not supported"
    }

    @Override
    public Connection connect( String url, Properties info ) throws SQLException {
      return null;
    }

    @Override
    public boolean acceptsURL( String url ) throws SQLException {
      return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
      return null;
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

  @Test
  public void connectTest() throws Exception {
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriverClass( CtorExceptionThrowingDriver.class ) ) );

    Assert.assertNull( "Fake driver should not make DriverManager hesitate", d.connect( null, null ) );
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
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.acceptsURL( null );
    Assert.assertTrue( called.get() );
  }

  @Test
  public void acceptsURL_no_driver() throws SQLException {
    HadoopShim shim = new MockHadoopShim() {
      @Override
      public Driver getJdbcDriver( String scheme ) {
        return null;
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( shim ) );

    Assert.assertFalse( d.acceptsURL( "jdbc:postgres://" ) );

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
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.getPropertyInfo( null, null );
    Assert.assertTrue( called.get() );
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
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.getMajorVersion();
    Assert.assertTrue( called.get() );
  }

  @Test
  public void getMajorVersion_exception() {
    Driver driver = new MockDriver() {
      @Override
      public int getMajorVersion() {
        throw new NullPointerException();
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    // If an exception is thrown the version returned should be -1
    Assert.assertEquals( -1, d.getMajorVersion() );
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
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.getMinorVersion();
    Assert.assertTrue( called.get() );
  }

  @Test
  public void getMinorVersion_exception() {
    Driver driver = new MockDriver() {
      @Override
      public int getMinorVersion() {
        throw new NullPointerException();
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    // If an exception is thrown the version returned should be -1
    Assert.assertEquals( -1, d.getMinorVersion() );
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
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.jdbcCompliant();
    Assert.assertTrue( called.get() );
  }

  @Test
  public void jdbcCompliant_exception() throws SQLException {
    Driver driver = new MockDriver() {
      @Override
      public boolean jdbcCompliant() {
        throw new NullPointerException();
      }
    };
    HiveDriver d = new HiveDriver( getMockUtil( getMockShimWithDriver( driver ) ) );

    // should return false if there is an exception
    Assert.assertFalse( d.jdbcCompliant() );
  }
}
