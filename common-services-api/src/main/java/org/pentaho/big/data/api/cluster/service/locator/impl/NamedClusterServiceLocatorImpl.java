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
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
@SuppressWarnings ( "WeakerAccess" )
public class NamedClusterServiceLocatorImpl implements NamedClusterServiceLocator {
  @VisibleForTesting final Map<String, Map<Class<?>, NamedClusterServiceFactory<?>>> serviceVendorTypeMapping;
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
      Map<Class<?>, NamedClusterServiceFactory<?>> classServiceMap =
        serviceVendorTypeMapping.get( shim );
      Objects.requireNonNull( classServiceMap )
        .put( namedClusterServiceFactory.getServiceClass(), namedClusterServiceFactory );
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

  @Override public <T> T getService( NamedCluster namedCluster, Class<T> serviceClass ) {
    Lock readLock = readWriteLock.readLock();
    try {
      readLock.lock();
      String shim = Objects.requireNonNull( getShimForService( namedCluster ) );
      logger.debug( "NamedClusterServiceLocator.getService({}, {})", namedCluster, serviceClass );
      Map<Class<?>, NamedClusterServiceFactory<?>> serviceMap =
        serviceVendorTypeMapping.getOrDefault( shim, Collections.emptyMap() );
      NamedClusterServiceFactory<?> serviceFactory = serviceMap.get( serviceClass );
      if ( serviceFactory != null && serviceFactory.canHandle( namedCluster ) ) {
        return serviceClass.cast( serviceFactory.create( namedCluster ) );
      }
    } finally {
      readLock.unlock();
    }
    logger.error( "Could not find service for {} associated with named cluster {}", serviceClass, namedCluster );
    return null;
  }

  /**
   * If namedCluster is defined, will use it to try to determine the
   * associated shim.  Otherwise returns the default shim name.
   */
  private String getShimForService( NamedCluster namedCluster ) {
    if ( namedCluster == null ) {
      return this.internalShim;
    }
    String shim = namedCluster.getShimIdentifier();
    NamedCluster storedNamedCluster =
      namedClusterManager.getNamedClusterByName( namedCluster.getName(), metastoreLocator.getMetastore() );
    if ( shim == null ) {
      if ( storedNamedCluster != null ) {
        shim = storedNamedCluster.getShimIdentifier();
      } else {
        shim = this.internalShim;
      }
    }
    return shim;
  }

  public List<String> getVendorShimList() {
    return new ArrayList<>( serviceVendorTypeMapping.keySet() );
  }

  /**
   * @deprecated to be removed once NamedClusterResolver is refactored.
   * If you see this post 9.0 kick @mkambol in the shins.
   */
  @Deprecated
  @Override public String getDefaultShim() {
    return internalShim;
  }

}
