/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Mimics HiveServer1 JDBC driver
 * The class is introduced to satisfy shims-common dependencies.
 * @author Mikhail_Tseu
 *
 */
public class HiveDriver implements Driver {
  private static final String SQL_STATE_NOT_SUPPORTED = "0A000";

  public HiveDriver() throws SQLException {
    throw new SQLException(
        "Currently active Hadoop shim does not support the HiveServer1 JDBC driver",
        SQL_STATE_NOT_SUPPORTED );
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
