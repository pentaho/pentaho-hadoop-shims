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


package org.pentaho.big.data.api.jdbc.impl;

import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrl;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collection;

/**
 * Created by bryan on 4/4/16.
 */
public class JdbcUrlParserImpl implements JdbcUrlParser {
  private NamedClusterService namedClusterService;
  private MetastoreLocator metastoreLocator;
  private final Logger logger = LoggerFactory.getLogger( JdbcUrlParserImpl.class );
  private static JdbcUrlParserImpl instance;

  public JdbcUrlParserImpl( NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  protected synchronized MetastoreLocator getMetastoreLocator() {
    MetastoreLocator metastoreLocator1;
    if ( this.metastoreLocator == null ) {
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        metastoreLocator1 = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        metastoreLocator1 = null;
        logger.error( "Error getting metastore locator", e );
      }
      metastoreLocator = metastoreLocator1;
    }
    return this.metastoreLocator;
  }

  @Override public JdbcUrl parse( String url ) throws URISyntaxException {
    return new JdbcUrlImpl( url, namedClusterService, getMetastoreLocator() );
  }

  public static JdbcUrlParserImpl getInstance() {
    if ( instance == null ) {
        try {
          Collection<NamedClusterService> namedClusterServices = PluginServiceLoader.loadServices( NamedClusterService.class );
          NamedClusterService service  = namedClusterServices.stream().findFirst().get();
          instance = new JdbcUrlParserImpl( service );
        } catch ( Exception e ) {

        }
    }
    return instance;
  }
}
