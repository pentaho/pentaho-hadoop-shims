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

/**
 * User: Dzmitry Stsiapanau Date: 7/2/2015 Time: 03:42
 */
public class ImpalaSimbaDriver extends HiveSimbaDriver {

  // Register ourself with the JDBC Driver Manager
  static {
    try {
      DriverManager.registerDriver( new ImpalaSimbaDriver() );
    } catch ( Exception ex ) {
      throw new RuntimeException( "Unable to register Impala Simba JDBC driver", ex );
    }
  }

  {
    METHOD_JDBC_PARAM = "ImpalaSimba";
    ERROR_SELF_DESCRIPTION = "Impala Simba";
  }

  /**
   * Create a new Imapala Simba driver with the default configuration utility.
   */
  public ImpalaSimbaDriver() {
    this( new HadoopConfigurationUtil() );
  }

  public ImpalaSimbaDriver( HadoopConfigurationUtil util ) {
    if ( util == null ) {
      throw new NullPointerException();
    }
    this.util = util;
  }
}
