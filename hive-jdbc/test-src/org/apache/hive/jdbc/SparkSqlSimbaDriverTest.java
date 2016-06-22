/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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


import org.junit.Assert;
import org.junit.Test;
import org.pentaho.hadoop.hive.jdbc.HadoopConfigurationUtil;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SparkSqlSimbaDriverTest extends HiveSimbaDriverTest {
  {
    SCHEME_STRING = "SparkSqlSimba";
    URL_UNSUITABLE = "jdbc:spark://host:port/";
    URL_SUITABLE = "jdbc:spark://host:port/AuthMech=0";
    errMsg = "Unable to load SparkSql Simba JDBC driver for the currently active Hadoop configuration";
  }

  @Override
  protected Driver getActiveTestDriverInstance( HadoopConfigurationUtil util ) {
    return new SparkSqlSimbaDriver( util );
  }

  @Override
  protected Driver getActiveTestDriverInstance() {
    return new SparkSqlSimbaDriver();
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

    d.connect( "jdbc:spark://host:port/AuthMech=0", null );
    Assert.assertTrue( called.get() );
    called.set( false );
    d.connect( "jdbc:hive2://host:port/AuthMech=0", null );
    Assert.assertFalse( called.get() );
  }
}
