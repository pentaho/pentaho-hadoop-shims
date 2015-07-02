/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

import org.pentaho.hadoop.hive.jdbc.HadoopConfigurationUtil;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * User: Dzmitry Stsiapanau Date: 7/2/2015 Time: 03:42
 */
public class HiveSimbaDriver extends HiveDriver {

  // Register ourself with the JDBC Driver Manager
  static {
    try {
      DriverManager.registerDriver( new HiveSimbaDriver() );
    } catch ( Exception ex ) {
      throw new RuntimeException( "Unable to register Hive Server 2 Simba JDBC driver", ex );
    }
  }

  {
    ERROR_SELF_DESCRIPTION = "Hive 2 Simba";
    METHOD_JDBC_PARAM = "hive2Simba";
  }

  /**
   * Create a new Hive Simba driver with the default configuration utility.
   */
  public HiveSimbaDriver() {
    this( new HadoopConfigurationUtil() );
  }

  public HiveSimbaDriver( HadoopConfigurationUtil util ) {
    if ( util == null ) {
      throw new NullPointerException();
    }
    this.util = util;
  }

  @Override
  protected boolean checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( !url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) ) {
      // BAD-215 check required to distinguish Simba driver
      return true;
    } else if ( getActiveDriver() == null ) {
      // Ignore connection attempt in case corresponding driver is not provided by the shim
      return true;
    }
    return false;
  }
}
