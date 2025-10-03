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


package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.spi.HBaseConnection;
import org.pentaho.hadoop.shim.spi.HBaseShim;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/2/16.
 */
public class HBaseServiceImplTest {
  private NamedCluster namedCluster;
  private HBaseServiceImpl hBaseService;
  private HBaseShim hBaseShim;
  private org.pentaho.hadoop.shim.spi.HBaseConnection hBaseConnection;
  private HBaseBytesUtilShim hBaseBytesUtilShim;

  @Before
  public void setup() throws Exception {
    namedCluster = mock( NamedCluster.class );
    hBaseShim = mock( HBaseShim.class );
    hBaseConnection = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );

    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    when( hBaseConnection.getBytesUtil() ).thenReturn( hBaseBytesUtilShim );
    when( namedCluster.getName() ).thenReturn( "namedClusterName" );
    when( namedCluster.getShimIdentifier() ).thenReturn( "shimidentifier" );
    when( namedCluster.isMapr() ).thenReturn( false );

    hBaseService = new HBaseServiceImpl( namedCluster, hBaseShim );
  }

  @Test
  public void testGetHBaseConnectionFull() throws Exception {
    String siteConfig = "site";
    String defaultConfig = "default";
    String zkHostRaw = "zkHostRaw";
    String zkHostFinal = "zkHostFinal";
    String zkPortRaw = "zkPortRaw";
    String zkPortFinal = "zkPortFinal";

    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    when( namedCluster.getZooKeeperHost() ).thenReturn( zkHostRaw );
    when( namedCluster.getZooKeeperPort() ).thenReturn( zkPortRaw );
    when( variableSpace.environmentSubstitute( zkHostRaw ) ).thenReturn( zkHostFinal );
    when( variableSpace.environmentSubstitute( zkPortRaw ) ).thenReturn( zkPortFinal );

    try (
      HBaseConnectionImpl hBaseConnection = hBaseService.getHBaseConnection( variableSpace, siteConfig, defaultConfig,
        logChannelInterface ) ) {
      hBaseConnection.checkHBaseAvailable();
    }
    ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass( Properties.class );
    verify( hBaseConnection )
      .configureConnection( propertiesArgumentCaptor.capture(), eq( namedCluster ), eq( new ArrayList<>() ) );
    Properties properties = propertiesArgumentCaptor.getValue();
    assertEquals( zkHostFinal, properties.get( HBaseConnection.ZOOKEEPER_QUORUM_KEY ) );
    assertEquals( zkPortFinal, properties.get( HBaseConnection.ZOOKEEPER_PORT_KEY ) );
    assertEquals( siteConfig, properties.get( HBaseConnection.SITE_KEY ) );
    assertEquals( defaultConfig, properties.get( HBaseConnection.DEFAULTS_KEY ) );
  }

  @Test
  public void testGetHBaseConnectionMinimal() throws Exception {
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    try ( HBaseConnectionImpl hBaseConnection = hBaseService
      .getHBaseConnection( variableSpace, null, null, logChannelInterface ) ) {
      hBaseConnection.checkHBaseAvailable();
    }
    ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass( Properties.class );
    verify( hBaseConnection )
      .configureConnection( propertiesArgumentCaptor.capture(), eq( namedCluster ), eq( new ArrayList <String>() ) );
    Properties properties = propertiesArgumentCaptor.getValue();
    assertEquals( 3, properties.size() );
  }

  @Test
  public void testGetColumnFilterFactory() {
    assertNotNull( hBaseService.getColumnFilterFactory() );
  }

  @Test
  public void testGetMappingFactory() {
    assertNotNull( hBaseService.getMappingFactory() );
  }

  @Test
  public void testGetHBaseValueMetaInterfaceFactory() {
    assertNotNull( hBaseService.getHBaseValueMetaInterfaceFactory() );
  }

  @Test
  public void testGetByteConversionUtil() {
    assertNotNull( hBaseService.getByteConversionUtil() );
  }

  @Test
  public void testGetResultFactory() {
    assertNotNull( hBaseService.getResultFactory() );
  }

  @Test
  public void testGetHBaseConnectionWithNullNamedCluster() throws Exception {
    String siteConfig = "site";
    String defaultConfig = "default";
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    hBaseService = new HBaseServiceImpl( null, hBaseShim );
    try {
      HBaseConnectionImpl hhBaseConnection =
        hBaseService.getHBaseConnection( variableSpace, siteConfig, defaultConfig, logChannelInterface );
      assertNotNull( hhBaseConnection );
    } catch ( NullPointerException e ) {
      fail( "No NPE is expected but it occurs" );
    }
  }
}
