/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Optional.ofNullable;

/**
 * Created by bryan on 11/5/15.
 */
@SuppressWarnings( "WeakerAccess" )
public class NamedClusterServiceLocatorImpl implements NamedClusterServiceLocator {
  @VisibleForTesting final Map<String, Map<Class<?>, List<NamedClusterServiceFactory<?>>>> serviceVendorTypeMapping;
  private final ReadWriteLock readWriteLock;
  @VisibleForTesting final String internalShim;
  private final MetastoreLocator metastoreLocator;
  private final NamedClusterService namedClusterManager;

  private static final Logger logger = LoggerFactory.getLogger( NamedClusterServiceLocatorImpl.class );

  public NamedClusterServiceLocatorImpl( String internalShim, MetastoreLocator metastoreLocator,
                                         NamedClusterService namedClusterManager ) {
    this.internalShim = Objects.requireNonNull(
      internalShim, "Set internal.shim in karaf/etc/pentaho.shim.cfg" );
    this.metastoreLocator = metastoreLocator;
    this.namedClusterManager = namedClusterManager;
    readWriteLock = new ReentrantReadWriteLock();
    serviceVendorTypeMapping = new HashMap<>();
  }

  public void factoryAdded( NamedClusterServiceFactory<?> namedClusterServiceFactory, Map properties ) {
    String shim = (String) properties.get( "shim" );
    if ( namedClusterServiceFactory == null
      || namedClusterServiceFactory.getServiceClass() == null
      || shim == null ) {
      logger.debug( "Undefined NamedClusterServiceFactory added." );
      return;
    }
    Lock writeLock = readWriteLock.writeLock();
    try {
      writeLock.lock();
      serviceVendorTypeMapping.putIfAbsent( shim, new HashMap<>() );
      Map<Class<?>, List<NamedClusterServiceFactory<?>>> classServiceMap =
        serviceVendorTypeMapping.get( shim );
      Class<?> serviceClass = namedClusterServiceFactory.getServiceClass();
      //Create the list of objects if not present
      classServiceMap.putIfAbsent( serviceClass, new ArrayList<>() );
      //Add the service Factory to the list
      Objects.requireNonNull( classServiceMap ).get( serviceClass ).add( namedClusterServiceFactory );
    } finally {
      writeLock.unlock();
    }
  }

  public void factoryRemoved( NamedClusterServiceFactory<?> namedClusterServiceFactory, Map properties ) {
    if ( namedClusterServiceFactory == null ) {
      logger.debug( "Undefined NamedClusterServiceFactory removed." );
      return;
    }
    Class<?> serviceClass = namedClusterServiceFactory.getServiceClass();
    Lock writeLock = readWriteLock.writeLock();
    try {
      writeLock.lock();
      String shim = (String) properties.get( "shim" );
      ofNullable( serviceVendorTypeMapping.get( shim ) )
        .ifPresent( serviceFactories -> serviceFactories.remove( serviceClass ) );
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public <T> T getService( NamedCluster namedCluster, Class<T> serviceClass ) {
    return getService( namedCluster, serviceClass, null );
  }

  @Override
  public <T> T getService( NamedCluster namedCluster, Class<T> serviceClass, String embeddedMetaStoreProviderKey ) {
    Lock readLock = readWriteLock.readLock();
    try {
      readLock.lock();
      String shim = Objects.requireNonNull( getShimForService( namedCluster, embeddedMetaStoreProviderKey ) );
      logger.debug( "NamedClusterServiceLocator.getService({}, {})", namedCluster, serviceClass );

      Map<Class<?>, List<NamedClusterServiceFactory<?>>> serviceMap = serviceVendorTypeMapping.get( shim );
      if ( serviceMap != null ) {
        List<NamedClusterServiceFactory<?>> serviceFactoryList = serviceMap.get( serviceClass );
        //We must have a list here because there can be multiple factories registered under the same shim and class
        //It is expected that the NamedClusterServiceFactory.canHandle( namedCluster ) method will determine which
        //factory is returned.  (eg: Both MapReduceImpersonationServiceFactor and KnoxMapReduceServiceFactor create
        //a MapReduceService.  But the knox factory should be returned for knox clusters and the impersonation factory
        //for all non-knox clusters.)
        if ( serviceFactoryList != null ) {
          for ( NamedClusterServiceFactory serviceFactory : serviceFactoryList ) {
            if ( serviceFactory.canHandle( namedCluster ) ) {
              return serviceClass.cast( serviceFactory.create( namedCluster ) );
            }
          }
        }
      }

    } finally {
      readLock.unlock();
    }
    logger.error( "Could not find service for {} associated with named cluster {}", serviceClass, namedCluster );
    return null;
  }

  /**
   * If namedCluster is defined, will use it to try to determine the associated shim.  Otherwise returns the default
   * shim name.
   */
  private String getShimForService( NamedCluster namedCluster, String embeddedMetaStoreProviderKey ) {
    if ( namedCluster == null ) {
      return this.internalShim;
    }
    String shim = namedCluster.getShimIdentifier();
    NamedCluster storedNamedCluster =
      namedClusterManager.getNamedClusterByName( namedCluster.getName(), metastoreLocator.getMetastore() );
    if ( shim == null && storedNamedCluster != null ) {
      shim = storedNamedCluster.getShimIdentifier();
    }
    if ( shim == null && storedNamedCluster == null && embeddedMetaStoreProviderKey != null ) {
      storedNamedCluster = namedClusterManager.getNamedClusterByName( namedCluster.getName(),
        metastoreLocator.getExplicitMetastore( embeddedMetaStoreProviderKey ) );
      if ( storedNamedCluster != null ) {
        shim = storedNamedCluster.getShimIdentifier();
      }
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
      shim = this.internalShim;
    }
    return shim;
  }

  public List<String> getVendorShimList() {
    return new ArrayList<>( serviceVendorTypeMapping.keySet() );
  }

}
