/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.pentaho.hadoop.hive.jdbc.HadoopConfigurationUtil;

/**
 * <p>
 * This is proxy driver for the Impala JDBC Driver available through the current active Hadoop configuration.
 * </p>
 * <p>
 * This driver is named exactly the same as the official Apache Hive driver so no further modifications are required by
 * calling code to swap in this proxy.
 * </p>
 * <p>
 * This class uses reflection to attempt to find the Big Data Plugin and load the HadoopConfigurationBootstrap so we
 * have access to the Hive JDBC driver that is compatible with the currently selected Hadoop configuration. All
 * operations are delegated to the current active Hadoop configuration's Hive JDBC driver via
 * HadoopConfiguration#getHiveJdbcDriver.
 * </p>
 * <p>
 * All calls to the loaded HiveDriver will have the current Thread's context class loader set to the class that loaded
 * the driver so subsequent resource lookups are successful.
 * </p>
 */
public class ImpalaDriver extends HiveDriver {
  /**
   * Method name of {@link org.pentaho.hadoop.shim.spi.HadoopShim#getJdbcDriver()}
   */
  private static final String METHOD_GET_JDBC_DRIVER = "getJdbcDriver";

  /**
   * Driver type = "hive"
   */
  private static final String METHOD_JDBC_PARAM = "hive2";

  /**
   * Utility for resolving Hadoop configurations dynamically.
   */
  private HadoopConfigurationUtil util;

  // Register ourself with the JDBC Driver Manager
  static {
    try {
      DriverManager.registerDriver( new ImpalaDriver() );
    } catch ( Exception ex ) {
      throw new RuntimeException( "Unable to register Impala JDBC driver", ex );
    }
  }

  /**
   * Create a new Hive driver with the default configuration utility.
   */
  public ImpalaDriver() {
    this( new HadoopConfigurationUtil() );
  }

  public ImpalaDriver( HadoopConfigurationUtil util ) {
    if ( util == null ) {
      throw new NullPointerException();
    }
    this.util = util;
  }

  protected Driver getActiveDriver() throws SQLException {
    Driver driver = null;
    try {
      Object shim = util.getActiveHadoopShim();
      Method getHiveJdbcDriver = shim.getClass().getMethod( METHOD_GET_JDBC_DRIVER, String.class );
      driver = (Driver) getHiveJdbcDriver.invoke( shim, METHOD_JDBC_PARAM );
    } catch ( Exception ex ) {
      throw new SQLException( "Unable to load Impala JDBC driver for the currently active Hadoop configuration", ex );
    }

    // Check if the Shim contains a Hive driver. It may return this driver if it
    // doesn't contain one since it'll be found in one of the parent class loaders
    // so we also need to make sure we didn't return ourself... :)
    if ( driver == null || driver.getClass() == this.getClass() ) {
      throw new SQLException( "The active Hadoop configuration does not contain a Impala JDBC driver" );
    }

    return driver;
  }

}
