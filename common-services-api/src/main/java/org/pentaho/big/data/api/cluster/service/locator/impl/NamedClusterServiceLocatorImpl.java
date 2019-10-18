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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.pentaho.big.data.api.shims.DefaultShim;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by bryan on 11/5/15.
 */
@SuppressWarnings ( "WeakerAccess" )
public class NamedClusterServiceLocatorImpl implements NamedClusterServiceLocator {
  @VisibleForTesting static final String SERVICE_RANKING = "service.ranking";
  private final Map<String, Multimap<Class<?>, ServiceFactoryAndRanking<?>>> serviceVendorTypeMapping;
  private final ReadWriteLock readWriteLock;
  private String fallbackShim;
  private String defaultShim;
  private final MetastoreLocator metastoreLocator;
  private final NamedClusterService namedClusterManager;
  private static final String NAMESPACE = "pentaho";

  private static final Logger logger = LoggerFactory.getLogger( NamedClusterServiceLocatorImpl.class );

  public NamedClusterServiceLocatorImpl( String fallbackShim, MetastoreLocator metastoreLocator,
                                         NamedClusterService namedClusterManager ) {
    this.fallbackShim = fallbackShim;
    this.metastoreLocator = metastoreLocator;
    this.namedClusterManager = namedClusterManager;
    readWriteLock = new ReentrantReadWriteLock();
    serviceVendorTypeMapping = new HashMap<>();
  }

  @VisibleForTesting
  Map<String, Multimap<Class<?>, ServiceFactoryAndRanking<?>>> getServiceVendorTypeMapping() {
    return serviceVendorTypeMapping;
  }

  public void factoryAdded( NamedClusterServiceFactory<?> namedClusterServiceFactory, Map properties ) {
    if ( namedClusterServiceFactory == null ) {
      return;
    }
    Class<?> serviceClass = namedClusterServiceFactory.getServiceClass();
    Lock writeLock = readWriteLock.writeLock();
    try {
      writeLock.lock();
      String shim = (String) properties.get( "shim" );

      Multimap<Class<?>, ServiceFactoryAndRanking<?>> serviceFactoryMap =
        Multimaps.newSortedSetMultimap( new HashMap<>(),
          () -> new TreeSet<>( ( o1, o2 ) -> {
            if ( o1.ranking == o2.ranking ) {
              return o1.namedClusterServiceFactory.toString().compareTo( o2.namedClusterServiceFactory.toString() );
            }
            return o2.ranking - o1.ranking;
          } ) );
      serviceVendorTypeMapping.putIfAbsent( shim, serviceFactoryMap );
      Multimap<Class<?>, ServiceFactoryAndRanking<?>> classMultiMap = serviceVendorTypeMapping.get( shim );
      classMultiMap.get( serviceClass ).add( new ServiceFactoryAndRanking( (Integer) properties.get( SERVICE_RANKING ),
        namedClusterServiceFactory ) );
    } finally {
      writeLock.unlock();
    }
  }

  public void factoryRemoved( NamedClusterServiceFactory<?> namedClusterServiceFactory, Map properties ) {
    if ( namedClusterServiceFactory == null ) {
      return;
    }
    Class<?> serviceClass = namedClusterServiceFactory.getServiceClass();
    Lock writeLock = readWriteLock.writeLock();
    try {
      writeLock.lock();
      String shim = (String) properties.get( "shim" );
      Optional.ofNullable( serviceVendorTypeMapping.get( shim ) ).ifPresent(
        v -> v.remove( serviceClass,
          new ServiceFactoryAndRanking( (Integer) properties.get( SERVICE_RANKING ), namedClusterServiceFactory ) )
      );
      housekeepShim( shim );
    } finally {
      writeLock.unlock();
    }
  }

  private void housekeepShim( String shim ) {
    if ( serviceVendorTypeMapping.containsKey( shim ) && serviceVendorTypeMapping.get( shim ).size() == 0 ) {
      serviceVendorTypeMapping.remove( shim );
    }
  }

  @Override public <T> T getService( NamedCluster namedCluster, Class<T> serviceClass ) {
    Lock readLock = readWriteLock.readLock();
    try {
      readLock.lock();
      String shim = getShimForService( namedCluster );

      if ( shim != null ) {
        Multimap<Class<?>, ServiceFactoryAndRanking<?>> multimap =
          serviceVendorTypeMapping.computeIfPresent( shim, ( key, value ) -> value );
        if ( multimap != null && multimap.get( serviceClass ) != null ) {
          for ( ServiceFactoryAndRanking<?> serviceFactoryAndRanking : multimap.get( serviceClass ) ) {
            if ( serviceFactoryAndRanking.namedClusterServiceFactory.canHandle( namedCluster ) ) {
              return serviceClass.cast( serviceFactoryAndRanking.namedClusterServiceFactory.create( namedCluster ) );
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return null;
  }

  /**
   * If namedCluster is defined, will use it to try to determine the
   * associated shim.  Otherwise returns the default shim name.
   */
  private String getShimForService( NamedCluster namedCluster ) {
    if ( namedCluster == null ) {
      return getDefaultShim();
    }
    String shim = namedCluster.getShimIdentifier();
    NamedCluster storedNamedCluster =
      namedClusterManager.getNamedClusterByName( namedCluster.getName(), metastoreLocator.getMetastore() );
    if ( ( shim == null ) && ( storedNamedCluster != null ) ) {
      shim = storedNamedCluster.getShimIdentifier();
    }
    if ( shim == null ) {
      // Named cluster is not fully defined in the metastore; might be a legacy configuration.
      try {
        shim = LegacyShimLocator.getLegacyDefaultShimName();
      } catch ( IOException e ) {
        // do nothing
      }
    }
    if ( shim == null ) {
      // No legacy configuration, use the default shim.
      shim = getDefaultShim();
    }
    return shim;
  }

  private String readDefaultShimFromMetastore() {
    IMetaStore metaStore = metastoreLocator.getMetastore();
    MetaStoreFactory metaStoreFactory =
      new MetaStoreFactory<>( DefaultShim.class, metaStore, NAMESPACE );
    try {
      DefaultShim defShim = ( (DefaultShim) metaStoreFactory.loadElement( "DefaultShim" ) );
      if ( defShim != null ) {
        return defShim.getDefaultShim();
      }
    } catch ( MetaStoreException e ) {
      logger.error( e.getMessage(), e );
    }
    return null;
  }

  private void writeDefaultShimToMetastore( String defaultShimValue ) {
    IMetaStore metaStore = metastoreLocator.getMetastore();
    MetaStoreFactory metaStoreFactory =
      new MetaStoreFactory<>( DefaultShim.class, metaStore, NAMESPACE );
    try {
      metaStoreFactory.saveElement( new DefaultShim( defaultShimValue ) );
    } catch ( MetaStoreException e ) {
      logger.error( e.getMessage(), e );
    }
  }

  public List<String> getVendorShimList() {
    return new ArrayList<>( serviceVendorTypeMapping.keySet() );
  }

  public String getDefaultShim() {
    if ( defaultShim != null ) {
      return defaultShim;
    }
    defaultShim = readDefaultShimFromMetastore();
    if ( defaultShim == null ) {
      setDefaultShim( fallbackShim );
    }

    return defaultShim;
  }

  public void setDefaultShim( String defaultShim ) {
    writeDefaultShimToMetastore( defaultShim );
    this.defaultShim = defaultShim;
  }

  static class ServiceFactoryAndRanking<T> {
    final int ranking;
    final NamedClusterServiceFactory<T> namedClusterServiceFactory;

    ServiceFactoryAndRanking( Integer ranking, NamedClusterServiceFactory<T> namedClusterServiceFactory ) {
      if ( ranking == null ) {
        this.ranking = 0;
      } else {
        this.ranking = ranking;
      }
      this.namedClusterServiceFactory = namedClusterServiceFactory;
    }
  }

}
