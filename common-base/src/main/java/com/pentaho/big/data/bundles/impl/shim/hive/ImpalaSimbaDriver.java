/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package com.pentaho.big.data.bundles.impl.shim.hive;

import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;

import java.sql.Driver;
import java.sql.SQLException;

/**
 * Created by bryan on 3/29/16.
 */
public class ImpalaSimbaDriver extends HiveSimbaDriver {
  public ImpalaSimbaDriver( JdbcUrlParser jdbcUrlParser,
                            String className, String shimVersion )
    throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    super( jdbcUrlParser, className, shimVersion, "ImpalaSimba" );
  }

  public ImpalaSimbaDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                            JdbcUrlParser jdbcUrlParser ) {
    super( delegate, hadoopConfigurationId, defaultConfiguration, jdbcUrlParser );
  }

  @Override
  protected Driver checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( !url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) || !url.matches( ".+:impala:.*" ) ) {
      // BAD-215 check required to distinguish Simba driver
      return null;
    }
    return delegate;
  }

  @Override
  protected boolean checkBeforeAccepting( String url ) {
    return ( hadoopConfigurationId != null )
      && url.matches( ".+:impala:.*" )
      && url.contains( SIMBA_SPECIFIC_URL_PARAMETER );
  }
}
