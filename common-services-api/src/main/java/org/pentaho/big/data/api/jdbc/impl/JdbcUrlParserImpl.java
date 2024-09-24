/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2023 by Hitachi Vantara : http://www.pentaho.com
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
  private final NamedClusterService namedClusterService;
  private MetastoreLocator metastoreLocator;
  private final Logger logger = LoggerFactory.getLogger( JdbcUrlParserImpl.class );

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
}
