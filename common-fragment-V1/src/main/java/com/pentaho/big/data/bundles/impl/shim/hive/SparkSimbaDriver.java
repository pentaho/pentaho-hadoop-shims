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

public class SparkSimbaDriver extends HiveSimbaDriver {
  public SparkSimbaDriver( JdbcUrlParser jdbcUrlParser,
                           String className, String shimVersion )
    throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    super( jdbcUrlParser, className, shimVersion, "SparkSqlSimba" );
  }

  public SparkSimbaDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                           JdbcUrlParser jdbcUrlParser ) {
    super( delegate, hadoopConfigurationId, defaultConfiguration, jdbcUrlParser );
  }

  @Override
  protected Driver checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( !url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) || !url.matches( ".+:spark:.*" ) ) {
      // BAD-215 check required to distinguish Simba driver
      return null;
    }
    return delegate;
  }

  protected boolean checkBeforeAccepting( String url ) {
    return url.matches( ".+:spark:.*" );
  }
}
