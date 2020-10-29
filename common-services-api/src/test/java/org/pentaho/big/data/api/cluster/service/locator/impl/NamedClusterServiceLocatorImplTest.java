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
  private Map<String, Map<Class<?>, List<NamedClusterServiceFactory<?>>>> serviceVendorTypeMapping;
  private NamedClusterServiceLocatorImpl serviceLocator;
  @Mock private NamedCluster namedCluster;
  @Mock private NamedCluster namedCluster2;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory2;
  @Mock private NamedClusterServiceFactory namedClusterServiceFactory3;
  @Mock private MetastoreLocator mockMetastoreLocator;
  @Mock private NamedClusterService namedClusterManager;
  private Object value = new Object();
  private String valueString = new String();
  MemoryMetaStore memoryMetaStore;


  @Before
  public void setup() {
    memoryMetaStore = new MemoryMetaStore();
    memoryMetaStore.setName( "memoryMetastore" );
    when( mockMetastoreLocator.getMetastore() ).thenReturn( memoryMetaStore );
    serviceLocator = new NamedClusterServiceLocatorImpl( SHIM_A, mockMetastoreLocator, namedClusterManager );
    serviceVendorTypeMapping = serviceLocator.serviceVendorTypeMapping;
    when( namedClusterServiceFactory.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory2.getServiceClass() ).thenReturn( String.class );
    when( namedClusterServiceFactory3.getServiceClass() ).thenReturn( Object.class );

    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( "0" );
    when( namedClusterServiceFactory2.create( namedCluster ) ).thenReturn( "2" );
    when( namedClusterServiceFactory3.create( namedCluster ) ).thenReturn( "3" );
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_A ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory, ImmutableMap.of( "shim", SHIM_B ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory2, ImmutableMap.of( "shim", SHIM_C ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory3, ImmutableMap.of( "shim", SHIM_A ) );
    when( namedClusterManager.getNamedClusterByName( namedCluster.getName(), memoryMetaStore ) ).thenReturn( namedCluster2 );
    when( namedClusterManager.getNamedClusterByName( namedCluster.getName(), null) ).thenReturn( null );
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
  public void testGetService() {
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
  }

  @Test
  public void testGetServiceWithAccessToEmbeddedMetastore() {
    String embeddedMetastoreKey = "theMetastoreKey";
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory2.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( value );
    when( mockMetastoreLocator.getMetastore() ).thenReturn( null );  //disable the normal metastore
    when( mockMetastoreLocator.getExplicitMetastore( embeddedMetastoreKey ) )
      .thenReturn( memoryMetaStore ); // and set to embedded metastore
    when( namedCluster2.getShimIdentifier() ).thenReturn( SHIM_C );
    assertEquals( "2", serviceLocator.getService( namedCluster, String.class, embeddedMetastoreKey ) );
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

  @Test
  public void testTwoFactoriesOnSameShimAndClass() {
    //When more than one FactoryService is registered under the same shim and class,
    // it is expected that the canHandle will pick the proper service factory for
    // the named cluster.
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( false );
    when( namedClusterServiceFactory3.canHandle( namedCluster ) ).thenReturn( true );

    Object service = serviceLocator.getService( namedCluster, Object.class );
    assertEquals( "3", service );

    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory3.canHandle( namedCluster ) ).thenReturn( false );

    service = serviceLocator.getService( namedCluster, Object.class );
    assertEquals( "0", service );
  }

}
