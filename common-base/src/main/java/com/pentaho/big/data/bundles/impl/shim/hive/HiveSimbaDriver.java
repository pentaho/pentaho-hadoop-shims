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


package com.pentaho.big.data.bundles.impl.shim.hive;

import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;

import java.sql.Driver;
import java.sql.SQLException;

/**
 * Created by bryan on 3/29/16.
 */
public class HiveSimbaDriver extends HiveDriver {

  public HiveSimbaDriver( JdbcUrlParser jdbcUrlParser,
                          String className, String shimVersion )
    throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    super( jdbcUrlParser, className, shimVersion, "hive2Simba" );
  }

  public HiveSimbaDriver( JdbcUrlParser jdbcUrlParser,
                          String className, String shimVersion, String driverType )
    throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    super( jdbcUrlParser, className, shimVersion, driverType );
  }

  public HiveSimbaDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                          JdbcUrlParser jdbcUrlParser ) {
    super( delegate, hadoopConfigurationId, defaultConfiguration, jdbcUrlParser );
  }

  @Override
  protected Driver checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( !url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) || !url.matches( ".+:hive2:.*" ) ) {
      // BAD-215 check required to distinguish Simba driver
      return null;
    }
    return delegate;
  }

  protected boolean checkBeforeAccepting( String url ) {
    return url.matches( ".+:hive2:.*" );
  }
}
