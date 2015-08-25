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

import org.junit.Test;
import org.pentaho.hadoop.hive.jdbc.HadoopConfigurationUtil;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class HiveSimbaDriverTest extends HiveDriverTest {

  {
    SCHEME_STRING = "hive2Simba";
    URL_UNSUITABLE = "jdbc.hive2://host:port/";
    URL_SUITABLE = "jdbc.hive2://host:port/AuthMech=0";
  }

  protected Driver getActiveTestDriverInstance( Driver driver ) {
    return getActiveTestDriverInstance( getMockUtil( getMockShimWithDriver( driver ) ) );
  }

  protected Driver getActiveTestDriverInstance( HadoopConfigurationUtil util ) {
    return new HiveSimbaDriver( util );
  }

  @Override protected Driver getActiveTestDriverInstance() {
    return new HiveSimbaDriver();
  }

  @Test
  @Override
  public void connectSimbaUrl() throws SQLException {
    final AtomicBoolean called = new AtomicBoolean( false );
    Driver driver = new MockDriver() {
      @Override
      public Connection connect( String url, Properties info ) throws SQLException {
        called.set( true );
        return null;
      }
    };
    Driver d = getActiveTestDriverInstance( getMockUtil( getMockShimWithDriver( driver ) ) );

    d.connect( "jdbc.hive2://host:port/AuthMech=0", null );
    assertTrue( called.get() );
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
      ( (HiveSimbaDriver) d ).getActiveDriver();
      fail( "Expected exception" );
    } catch ( SQLException ex ) {
      assertEquals( InvocationTargetException.class, ex.getCause().getClass() );
      assertEquals( "Unable to load Hive 2 Simba JDBC driver for the currently active Hadoop configuration",
        ex.getMessage() );
    }
  }
}
