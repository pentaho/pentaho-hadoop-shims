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

package org.pentaho.big.data.impl.shim.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 12/8/15.
 */
public class MapReduceServiceFactoryImplTest {
  private boolean isActiveConfiguration;
  private ExecutorService executorService;
  private MapReduceServiceFactoryImpl mapReduceServiceFactory;
  private NamedCluster namedCluster;
  private List<TransformationVisitorService> visitorServices = new ArrayList<>();

  @Before
  public void setup() {
    isActiveConfiguration = true;
    executorService = mock( ExecutorService.class );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testGetServiceClass() {
    assertEquals( MapReduceService.class, mapReduceServiceFactory.getServiceClass() );
  }

  @Test
  public void testCanHandleActive() {
    assertTrue( mapReduceServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCanHandleInactive() {
    isActiveConfiguration = false;
//    assertFalse( mapReduceServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCreate() {
    assertTrue( mapReduceServiceFactory.create( namedCluster ) instanceof MapReduceServiceImpl );
  }
}
