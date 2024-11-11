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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.spi.HBaseConnection;
import org.pentaho.hadoop.shim.spi.HBaseShim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/2/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class HBaseServiceFactoryTest {

  private HBaseServiceFactory hBaseServiceFactory;
  @Mock
  HBaseShim hBaseShim;

  @Before
  public void setUp() {
    hBaseServiceFactory = new HBaseServiceFactory( true, hBaseShim );
  }

  @Test
  public void testGetServiceClass() {
    assertEquals( HBaseService.class, hBaseServiceFactory.getServiceClass() );
  }

  @Test
  public void testCanHandle() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    assertTrue( hBaseServiceFactory.canHandle( namedCluster ) );
    assertTrue( new HBaseServiceFactory( false, hBaseShim ).canHandle( namedCluster ) );
  }

  @Test
  public void testCreateSuccess() throws Exception {
    HBaseConnection hBaseConnection = mock( HBaseConnection.class );

    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    when( hBaseConnection.getBytesUtil() ).thenReturn( mock( HBaseBytesUtilShim.class ) );

    assertTrue( hBaseServiceFactory.create( mock( NamedCluster.class ) ) instanceof HBaseServiceImpl );
  }

  @Test
  public void testCreateFalure() throws ConfigurationException {
    HBaseShim hBaseShim = mock( HBaseShim.class );

    assertNull( hBaseServiceFactory.create( mock( NamedCluster.class ) ) );
  }
}
