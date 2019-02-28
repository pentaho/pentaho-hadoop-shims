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

package org.pentaho.big.data.impl.shim.sqoop;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.sqoop.SqoopService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/26/16.
 */
public class SqoopServiceFactoryImplTest {
  private boolean isActiveConfiguration;
  private SqoopServiceFactoryImpl sqoopServiceFactory;
  private NamedCluster namedCluster;

  @Before
  public void setup() throws ConfigurationException {
    isActiveConfiguration = true;
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testCanHandle() throws ConfigurationException {
    assertTrue( sqoopServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCannotHandleGateway() throws Exception {
    when( namedCluster.isUseGateway() ).thenReturn( true );
    //todo: fix knox separately - canHandle
    //assertFalse( sqoopServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testGetServiceClass() {
    assertEquals( SqoopService.class, sqoopServiceFactory.getServiceClass() );
  }

  @Test
  public void testCreateSuccess() {
    assertNotNull( sqoopServiceFactory.create( namedCluster ) );
  }

}
