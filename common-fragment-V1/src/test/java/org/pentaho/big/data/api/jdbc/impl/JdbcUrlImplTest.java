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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/14/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class JdbcUrlImplTest {
  @Mock NamedClusterService namedClusterService;
  @Mock MetastoreLocator metastoreLocator;

  @Test
  public void testNoParameterRoundTrip() throws URISyntaxException {
    String host = "my.hadoop.cluster";
    String url = "jdbc:hive2://" + host + ":999/default";
    JdbcUrlImpl jdbcUrl = new JdbcUrlImpl( url, namedClusterService, metastoreLocator );
    assertEquals( url, jdbcUrl.toString() );
    assertEquals( host, jdbcUrl.getHost() );
  }

  @Test( expected = URISyntaxException.class )
  public void testNoJdbcPrefix() throws URISyntaxException {
    new JdbcUrlImpl( "hive2://my.hadoop.cluster:999/default", namedClusterService, metastoreLocator );
  }

  @Test
  public void testInitialParameter() throws URISyntaxException {
    String auth = "Auth";
    String test = "test";
    String url = "jdbc:hive2://my.hadoop.cluster:999/default;" + auth + "=" + test;
    JdbcUrlImpl jdbcUrl = new JdbcUrlImpl( url, namedClusterService, metastoreLocator );
    assertEquals( url, jdbcUrl.toString() );
    assertEquals( test, jdbcUrl.getQueryParam( auth ) );
  }

  @Test
  public void testSetParameter() throws URISyntaxException {
    String auth = "Auth";
    String test = "test";
    String baseUrl = "jdbc:hive2://my.hadoop.cluster:999/default";
    String expectedUrl = baseUrl + ";" + auth + "=" + test;
    JdbcUrlImpl jdbcUrl = new JdbcUrlImpl( baseUrl, namedClusterService, metastoreLocator );
    jdbcUrl.setQueryParam( auth, test );
    assertEquals( expectedUrl, jdbcUrl.toString() );
    assertEquals( test, jdbcUrl.getQueryParam( auth ) );
  }

  @Test
  public void testGetNamedClusterNullMetastore() throws MetaStoreException, URISyntaxException {
    String myCluster = "myCluster";
    String url = "jdbc:hive2://my.hadoop.cluster:999/default;" + JdbcUrlImpl.PENTAHO_NAMED_CLUSTER + "=" + myCluster;
    when( metastoreLocator.getMetastore() ).thenReturn( null );
    JdbcUrlImpl jdbcUrl = new JdbcUrlImpl( url, namedClusterService, metastoreLocator );
    assertEquals( myCluster, jdbcUrl.getQueryParam( JdbcUrlImpl.PENTAHO_NAMED_CLUSTER ) );
    assertNull( jdbcUrl.getNamedCluster() );
  }

  @Test
  public void testGetNamedClusterNullQueryParam() throws MetaStoreException, URISyntaxException {
    String url = "jdbc:hive2://my.hadoop.cluster:999/default";
    when( metastoreLocator.getMetastore() ).thenReturn( mock( IMetaStore.class ) );
    JdbcUrlImpl jdbcUrl = new JdbcUrlImpl( url, namedClusterService, metastoreLocator );
    assertNull( jdbcUrl.getQueryParam( JdbcUrlImpl.PENTAHO_NAMED_CLUSTER ) );
    assertNull( jdbcUrl.getNamedCluster() );
  }

  @Test
  public void testGetNamedClusterSuccess() throws MetaStoreException, URISyntaxException {
    String myCluster = "myCluster";
    String url = "jdbc:hive2://my.hadoop.cluster:999/default;" + JdbcUrlImpl.PENTAHO_NAMED_CLUSTER + "=" + myCluster;
    IMetaStore iMetaStore = mock( IMetaStore.class );
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedClusterService.read( myCluster, iMetaStore ) ).thenReturn( namedCluster );
    when( metastoreLocator.getMetastore() ).thenReturn( iMetaStore );
    JdbcUrlImpl jdbcUrl = new JdbcUrlImpl( url, namedClusterService, metastoreLocator );
    assertEquals( myCluster, jdbcUrl.getQueryParam( JdbcUrlImpl.PENTAHO_NAMED_CLUSTER ) );
    assertEquals( namedCluster, jdbcUrl.getNamedCluster() );
  }
}
