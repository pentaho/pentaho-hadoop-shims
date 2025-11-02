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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 4/14/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class JdbcUrlParserImplTest {
  @Mock
  NamedClusterService namedClusterService;
  @Mock
  MetastoreLocator metastoreLocator;
  JdbcUrlParserImpl jdbcUrlParser;
  JdbcUrlParserImpl jdbcUrlParserWithGetInstance;

  MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic;

  @Before
  public void setup() {
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( metastoreLocator );
    pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class );
    pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
      .thenReturn( metastoreLocatorCollection );

    Collection<NamedClusterService> namedClusterServicesCollection = new ArrayList<>();
    namedClusterServicesCollection.add( namedClusterService );
    pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( NamedClusterService.class ) )
      .thenReturn( namedClusterServicesCollection );
    jdbcUrlParserWithGetInstance = JdbcUrlParserImpl.getInstance();
    jdbcUrlParser = new JdbcUrlParserImpl( namedClusterService );
  }

  @After
  public void tearDown() {
    if ( pluginServiceLoaderMockedStatic != null ) {
      pluginServiceLoaderMockedStatic.close();
    }
  }

  @Test
  public void testParse() throws URISyntaxException {
    assertTrue( jdbcUrlParser.parse( "jdbc:hive2://host:80/default" ) instanceof JdbcUrlImpl );
  }

  @Test
  public void testParseWithGetInstance() throws URISyntaxException {
    assertTrue( jdbcUrlParserWithGetInstance.parse( "jdbc:hive2://host:80/default" ) instanceof JdbcUrlImpl );
  }
}
