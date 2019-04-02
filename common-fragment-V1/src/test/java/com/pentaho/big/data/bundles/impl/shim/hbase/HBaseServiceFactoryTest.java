/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.spi.HBaseConnection;
import org.pentaho.hadoop.shim.spi.HBaseShim;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/2/16.
 */
public class HBaseServiceFactoryTest {

  private HBaseServiceFactory hBaseServiceFactory;

  @Test
  public void testGetServiceClass() {
    //assertEquals( HBaseService.class, hBaseServiceFactory.getServiceClass() );
  }

  @Test
  public void testCanHandle() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    //assertTrue( hBaseServiceFactory.canHandle( namedCluster ) );
    //assertFalse( new HBaseServiceFactory( false, hadoopConfiguration ).canHandle( namedCluster ) );
  }

  @Test
  public void testCreateSuccess() throws Exception {
    HBaseShim hBaseShim = mock( HBaseShim.class );
    HBaseConnection hBaseConnection = mock( HBaseConnection.class );

    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    when( hBaseConnection.getBytesUtil() ).thenReturn( mock( HBaseBytesUtilShim.class ) );

    //assertTrue( hBaseServiceFactory.create( mock( NamedCluster.class ) ) instanceof HBaseServiceImpl );
  }

  @Test
  public void testCreateFalure() throws ConfigurationException {
    HBaseShim hBaseShim = mock( HBaseShim.class );

    //assertNull( hBaseServiceFactory.create( mock( NamedCluster.class ) ) );
  }
}
