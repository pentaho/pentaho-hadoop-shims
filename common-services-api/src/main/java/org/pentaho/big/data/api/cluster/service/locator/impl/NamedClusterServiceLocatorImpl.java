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


package org.pentaho.big.data.api.cluster.service.locator.impl;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
  private MetastoreLocator metastoreLocator;
  private final NamedClusterService namedClusterManager;
  private static NamedClusterServiceLocatorImpl namedClusterServiceLocator = null;

  private static final Logger logger = LoggerFactory.getLogger( NamedClusterServiceLocatorImpl.class );

  protected NamedClusterServiceLocatorImpl( String internalShim, NamedClusterService namedClusterManager ) {
    this.internalShim = Objects.requireNonNull(
      internalShim, "Set internal.shim in karaf/etc/pentaho.shim.cfg" );
    this.namedClusterManager = namedClusterManager;
    readWriteLock = new ReentrantReadWriteLock();
    serviceVendorTypeMapping = new HashMap<>();
  }

  public static synchronized NamedClusterServiceLocatorImpl getInstance() {
    try {
      if (namedClusterServiceLocator == null) {
        String shimIdentifier = "NONE";
        if ( HadoopConfigurationBootstrap.getInstance().getProvider() != null ) {
          shimIdentifier = HadoopConfigurationBootstrap.getInstance().getProvider()
                  .getActiveConfiguration().getIdentifier();
        }
        namedClusterServiceLocator = new NamedClusterServiceLocatorImpl(
                shimIdentifier,
                NamedClusterManager.getInstance()
        );
      }
    } catch (ConfigurationException e) {
      // TODO: Handle runtime exception better
        throw new RuntimeException(e);
    }
      return namedClusterServiceLocator;
  }

  protected synchronized MetastoreLocator getMetastoreLocator() {
    if ( this.metastoreLocator == null ) {
      MetastoreLocator metastoreLocator1;
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        metastoreLocator1 = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        metastoreLocator1 = null;
        logger.error( "Error getting metastore locator", e );
      }
      metastoreLocator = metastoreLocator1;
    }
    return this.metastoreLocator;
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
    if ( shim != null ) {
      return shim;
    }
    NamedCluster storedNamedCluster =
      namedClusterManager.getNamedClusterByName( namedCluster.getName(), getMetastoreLocator().getMetastore() );
    if ( storedNamedCluster != null ) {
      shim = storedNamedCluster.getShimIdentifier();
    }
    if ( shim == null && storedNamedCluster == null && embeddedMetaStoreProviderKey != null ) {
      storedNamedCluster = namedClusterManager.getNamedClusterByName( namedCluster.getName(),
        getMetastoreLocator().getExplicitMetastore( embeddedMetaStoreProviderKey ) );
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
