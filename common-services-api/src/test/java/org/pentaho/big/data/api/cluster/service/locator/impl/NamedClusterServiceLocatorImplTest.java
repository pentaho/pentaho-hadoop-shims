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

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 11/6/15.
 */
@RunWith ( MockitoJUnitRunner.class )
public class NamedClusterServiceLocatorImplTest {
  private static final String SHIM_A = "shimA";
  private static final String SHIM_B = "shimB";
  private static final String SHIM_C = "shimC";
  private Map<String, Map<Class<?>, NamedClusterServiceFactory<?>>> serviceVendorTypeMapping;
  private NamedClusterServiceLocatorImpl serviceLocator;
  @Mock private NamedCluster namedCluster;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory2;
  @Mock private MetastoreLocator mockMetastoreLocator;
  @Mock private NamedClusterService namedClusterManager;
  private Object value = new Object();


  @Before
  public void setup() {
    MemoryMetaStore memoryMetaStore = new MemoryMetaStore();
    memoryMetaStore.setName( "memoryMetastore" );
    when( mockMetastoreLocator.getMetastore() ).thenReturn( memoryMetaStore );
    serviceLocator = new NamedClusterServiceLocatorImpl( SHIM_A, mockMetastoreLocator, namedClusterManager );
    serviceVendorTypeMapping = serviceLocator.serviceVendorTypeMapping;
    when( namedClusterServiceFactory.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory2.getServiceClass() ).thenReturn( String.class );
    when( namedClusterServiceFactory.toString() ).thenReturn( "d" );
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_B ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory2, ImmutableMap.of( "shim", SHIM_C ) );
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

    assertNotNull( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );
    assertNotNull( serviceVendorTypeMapping.get( SHIM_B ).get( Object.class ) );
    assertNotNull( serviceVendorTypeMapping.get( SHIM_C ).get( String.class ) );

    //Remove one of the factories
    serviceLocator.factoryRemoved( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A ) );
    assertNull( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );
    //Try the same removal again
    serviceLocator.factoryRemoved( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A ) );
    assertNull( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );

    // add it back
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A ) );
    assertNotNull( serviceVendorTypeMapping.get( SHIM_A ).get( Object.class ) );

    // others are still present
    assertNotNull( serviceVendorTypeMapping.get( SHIM_B ).get( Object.class ) );
    assertNotNull( serviceVendorTypeMapping.get( SHIM_C ).get( String.class ) );
  }

  @Test
  public void testGetServiceFirst() {
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
  }

  @Test
  public void testGetServiceLast() {
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
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
