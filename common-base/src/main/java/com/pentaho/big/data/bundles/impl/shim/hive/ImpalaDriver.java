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

/**
 * Created by bryan on 3/29/16.
 */
public class ImpalaDriver extends HiveDriver {
  public ImpalaDriver( JdbcUrlParser jdbcUrlParser,
                       String className, String shimVersion )
    throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    super( jdbcUrlParser, className, shimVersion, "Impala" );
  }

  public ImpalaDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                       JdbcUrlParser jdbcUrlParser ) {
    super( delegate, hadoopConfigurationId, defaultConfiguration, jdbcUrlParser );
  }

  @Override
  protected boolean checkBeforeAccepting( String url ) {
    return ( hadoopConfigurationId != null )
      && url.matches( ".+:impala:.*" )
      && !url.contains( SIMBA_SPECIFIC_URL_PARAMETER );
  }
}
