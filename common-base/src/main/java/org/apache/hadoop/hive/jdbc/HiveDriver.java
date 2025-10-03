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


package org.apache.hadoop.hive.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Mimics HiveServer1 JDBC driver The class is introduced to satisfy shims-common dependencies.
 *
 * @author Mikhail_Tseu
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
