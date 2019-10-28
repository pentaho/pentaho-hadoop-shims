/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.api.cluster.service.locator.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl.SERVICE_RANKING;

/**
 * Created by bryan on 11/6/15.
 */
@RunWith ( MockitoJUnitRunner.class )
public class NamedClusterServiceLocatorImplTest {
  private static final String SHIM_A = "shimA";
  private static final String SHIM_B = "shimB";
  private Map<String, Multimap<Class<?>, NamedClusterServiceLocatorImpl.ServiceFactoryAndRanking<?>>>
    serviceVendorTypeMapping;
  private NamedClusterServiceLocatorImpl serviceLocator;
  @Mock private NamedCluster namedCluster;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory2;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory3;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory4;
  @Mock private MetastoreLocator mockMetastoreLocator;
  @Mock private NamedClusterService namedClusterManager;
  private Object value = new Object();


  @Before
  public void setup() {
    MemoryMetaStore memoryMetaStore = new MemoryMetaStore();
    memoryMetaStore.setName( "memoryMetastore" );
    when( mockMetastoreLocator.getMetastore() ).thenReturn( memoryMetaStore );
    serviceLocator = new NamedClusterServiceLocatorImpl( SHIM_A, mockMetastoreLocator, namedClusterManager );
    serviceVendorTypeMapping = serviceLocator.getServiceVendorTypeMapping();
    when( namedClusterServiceFactory.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory2.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory3.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory4.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory.toString() ).thenReturn( "d" );
    when( namedClusterServiceFactory2.toString() ).thenReturn( "b" );
    when( namedClusterServiceFactory3.toString() ).thenReturn( "a" );
    when( namedClusterServiceFactory4.toString() ).thenReturn( "c" );
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 2 ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory2, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 4 ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory3, Collections.emptyMap() );
    serviceLocator.factoryAdded( namedClusterServiceFactory4, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 4 ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_B, SERVICE_RANKING, 3 ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory4, ImmutableMap.of( "shim", SHIM_B, SERVICE_RANKING, 5 ) );
  }

  @Test
  public void testNoArgConstructor() {
    assertNull( new NamedClusterServiceLocatorImpl( SHIM_A, mockMetastoreLocator, namedClusterManager )
      .getService( namedCluster, Object.class ) );
    assertEquals( SHIM_A, serviceLocator.internalShim );
    serviceLocator.getVendorShimList();
  }

  @Test
  public void testFactoryAddedRemoved() {
    List<String> shims = serviceLocator.getVendorShimList();
    assertEquals( 3, shims.size() );
    List<NamedClusterServiceLocatorImpl.ServiceFactoryAndRanking<?>> serviceFactoryAndRankings =
      new ArrayList<>( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );
    assertEquals( 3, serviceFactoryAndRankings.size() );
    //Factories should be ordered by ranking
    assertEquals( namedClusterServiceFactory2, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory, serviceFactoryAndRankings.get( 2 ).namedClusterServiceFactory );

    serviceFactoryAndRankings = new ArrayList<>( serviceVendorTypeMapping.get( SHIM_B ).get( Object.class ) );
    assertEquals( 2, serviceFactoryAndRankings.size() );
    //Factories should be ordered by ranking
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );

    //Remove one of the factories
    serviceLocator.factoryRemoved( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 2 ) );
    serviceFactoryAndRankings = new ArrayList<>( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );
    assertEquals( 2, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory2, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );

    //Try the same removal again
    serviceLocator.factoryRemoved( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 2 ) );
    serviceFactoryAndRankings = new ArrayList<>( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );
    assertEquals( 2, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory2, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );

    serviceLocator
      .factoryRemoved( namedClusterServiceFactory2, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 4 ) );
    serviceFactoryAndRankings = new ArrayList<>( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );
    assertEquals( 1, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );

    serviceLocator
      .factoryRemoved( namedClusterServiceFactory4, ImmutableMap.of( "shim", SHIM_A, SERVICE_RANKING, 4 ) );
    assertFalse( serviceVendorTypeMapping.containsKey( SHIM_A ) );
  }

  @Test
  public void testGetServiceFirst() {
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
    verify( namedClusterServiceFactory2, never() ).create( namedCluster );
    verify( namedClusterServiceFactory3, never() ).create( namedCluster );
    verify( namedClusterServiceFactory4, never() ).create( namedCluster );
  }

  @Test
  public void testGetServiceLast() {
    when( namedClusterServiceFactory4.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory4.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
    verify( namedClusterServiceFactory, never() ).create( namedCluster );
    verify( namedClusterServiceFactory2, never() ).create( namedCluster );
    verify( namedClusterServiceFactory3, never() ).create( namedCluster );
  }

  @Test
  public void testGetServiceWithNullNamedCluster() {
    when( namedClusterServiceFactory.canHandle( null ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( null ) ).thenReturn( value );
    Object service = serviceLocator.getService( null, Object.class );
    assertNotNull( service );
  }

  @Test
  public void testNoServiceFound() {
    Object service = serviceLocator.getService( null, FormatService.class );
    assertNull( service );
  }


}
