/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.mapr31.authorization;

import java.io.IOException;
import java.sql.Driver;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.hadoop.security.HadoopKerberosName;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.di.core.auth.core.AuthenticationFactoryException;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.DistributedCacheUtil;
import org.pentaho.hadoop.shim.common.CommonSqoopShim;
import org.pentaho.hadoop.shim.common.ShimUtils;
import org.pentaho.hadoop.shim.mapr31.MapR3DistributedCacheUtilImpl;
import org.pentaho.hadoop.shim.mapr31.authentication.HiveKerberosConsumer;
import org.pentaho.hadoop.shim.mapr31.authentication.PropertyAuthenticationProviderParser;
import org.pentaho.hadoop.shim.mapr31.delegatingShims.DelegatingHadoopShim;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.mapr31.authentication.HBaseKerberosConsumer;
import org.pentaho.hbase.shim.mapr31.wrapper.HBaseShimInterface;
import org.pentaho.hdfs.vfs.HadoopFileSystem;
import org.pentaho.hdfs.vfs.MapRFileProvider;
import org.pentaho.hdfs.vfs.MapRFileSystem;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientFactory;
import org.pentaho.oozie.shim.api.OozieJob;
import org.pentaho.oozie.shim.mapr31.OozieClientFactoryImpl;

public class UserSpoofingHadoopAuthorizationService extends NoOpHadoopAuthorizationService implements
    HadoopAuthorizationService {
  protected static final String HDFS_PROXY_USER = "pentaho.hdfs.proxy.user";
  protected static final String MR_PROXY_USER = "pentaho.mapreduce.proxy.user";
  protected static final String PIG_PROXY_USER = "pentaho.pig.proxy.user";
  protected static final String SQOOP_PROXY_USER = "pentaho.sqoop.proxy.user";
  protected static final String OOZIE_PROXY_USER = "pentaho.oozie.proxy.user";

  private final Map<Class<?>, String> userMap;
  private final Map<Class<?>, Set<Class<?>>> delegateMap;
  private final Map<Class<?>, PentahoHadoopShim> shimMap;
  private final UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable;
  private final HadoopShim hadoopShim;
  private final OozieClientFactory oozieClientFactory;
  private final HBaseShimInterface hBaseShimInterface;
  private final SqoopShim sqoopShim;
  private boolean isRoot;

  public UserSpoofingHadoopAuthorizationService(
      final UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable )
    throws AuthenticationConsumptionException {
    this.userSpoofingHadoopAuthorizationCallable = userSpoofingHadoopAuthorizationCallable;
    isRoot = this.userSpoofingHadoopAuthorizationCallable.call().getUserCreds().getIsRoot();
    userMap = new HashMap<Class<?>, String>();
    userMap.put( PigShim.class, PIG_PROXY_USER );
    userMap.put( OozieClientFactory.class, OOZIE_PROXY_USER );
    shimMap = new HashMap<Class<?>, PentahoHadoopShim>();
    delegateMap = new HashMap<Class<?>, Set<Class<?>>>();
    Set<Class<?>> oozieSet = new HashSet<Class<?>>( Arrays.<Class<?>> asList( OozieClient.class, OozieJob.class ) );
    delegateMap.put( OozieClientFactory.class, oozieSet );
    hadoopShim = UserSpoofingMaprInvocationHandler.forObject( new org.pentaho.hadoop.shim.mapr31.HadoopShim() {

      public String getFileSystemGetUser( Configuration conf ) {
        return conf.get( HDFS_PROXY_USER );
      }

      @SuppressWarnings( "unused" )
      public String submitJobGetUser( Configuration conf ) {
        return conf.get( MR_PROXY_USER );
      }

      @SuppressWarnings( "unused" )
      public String getDistributedCacheUtilGetUser() throws ConfigurationException {
        return createConfiguration().get( HDFS_PROXY_USER );
      }

      @Override
      public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
        fsm.addProvider( config, MapRFileProvider.SCHEME, config.getIdentifier(), new MapRFileProvider() {

          @Override
          protected FileSystem doCreateFileSystem( FileName name, FileSystemOptions fileSystemOptions )
            throws FileSystemException {
            return new MapRFileSystem( name, fileSystemOptions ) {

              @Override
              public HadoopFileSystem getHDFSFileSystem() throws FileSystemException {
                Callable<HadoopFileSystem> callable = new Callable<HadoopFileSystem>() {

                  @Override
                  public HadoopFileSystem call() throws Exception {
                    return getHDFSFileSystemPrivate();
                  }
                };
                try {
                  return UserSpoofingMaprInvocationHandler.forObject( callable, new HashSet<Class<?>>(),
                      getFileSystemGetUser( createConfiguration() ), isRoot );
                } catch ( AuthenticationConsumptionException e ) {
                  throw new FileSystemException( e.getCause() );
                }
              }

              private HadoopFileSystem getHDFSFileSystemPrivate() throws FileSystemException {
                return super.getHDFSFileSystem();
              }
            };
          }
        } );
        setDistributedCacheUtil( new MapR3DistributedCacheUtilImpl( config ) );
      }

      @Override
      public Driver getJdbcDriver( String driverType ) {
        Driver delegate = super.getJdbcDriver( driverType );
        AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
        try {
          manager.registerConsumerClass( HiveKerberosConsumer.class );
        } catch ( AuthenticationFactoryException e ) {
          throw new RuntimeException( e );
        }
        new PropertyAuthenticationProviderParser( userSpoofingHadoopAuthorizationCallable.getConfigProperties(),
            manager ).process( DelegatingHadoopShim.PROVIDER_LIST );
        AuthenticationPerformer<Driver, Driver> performer =
            manager.getAuthenticationPerformer( Driver.class, Driver.class, userSpoofingHadoopAuthorizationCallable
                .getConfigProperties().getProperty( DelegatingHadoopShim.SUPER_USER ) );
        if ( performer != null ) {
          try {
            return performer.perform( delegate );
          } catch ( AuthenticationConsumptionException e ) {
            throw new RuntimeException( e );
          }
        } else {
          throw new RuntimeException( "Unable to find authentication performer for id "
              + userSpoofingHadoopAuthorizationCallable.getConfigProperties().getProperty(
                  DelegatingHadoopShim.SUPER_USER ) + " (specified as " + DelegatingHadoopShim.SUPER_USER
              + " in config.properties)", null );
        }
      }
    }, new HashSet<Class<?>>( Arrays.<Class<?>> asList( DistributedCacheUtil.class ) ) );

    try {
      HadoopKerberosName.setConfiguration( ShimUtils.asConfiguration( hadoopShim.createConfiguration() ) );
    } catch ( IOException e1 ) {
      throw new AuthenticationConsumptionException( e1 );
    }
    oozieClientFactory =
        KerberosInvocationHandler.forObject( userSpoofingHadoopAuthorizationCallable.getLoginContext(),
            new OozieClientFactoryImpl( hadoopShim.createConfiguration().get( OOZIE_PROXY_USER ) ),
            new HashSet<Class<?>>( Arrays.<Class<?>> asList( OozieClientFactory.class, OozieClient.class,
                OozieJob.class ) ) );
    AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
    try {
      manager.registerConsumerClass( HBaseKerberosConsumer.class );
    } catch ( AuthenticationFactoryException e ) {
      throw new AuthenticationConsumptionException( e );
    }
    new PropertyAuthenticationProviderParser( userSpoofingHadoopAuthorizationCallable.getConfigProperties(), manager )
        .process( DelegatingHadoopShim.PROVIDER_LIST );
    AuthenticationPerformer<HBaseShimInterface, Void> performer =
        manager
            .getAuthenticationPerformer( HBaseShimInterface.class, Void.class, userSpoofingHadoopAuthorizationCallable
                .getConfigProperties().getProperty( DelegatingHadoopShim.SUPER_USER ) );
    if ( performer != null ) {
      hBaseShimInterface = performer.perform( null );
    } else {
      throw new AuthenticationConsumptionException( "Unable to find authentication performer for id "
          + userSpoofingHadoopAuthorizationCallable.getConfigProperties().getProperty(
              DelegatingHadoopShim.SUPER_USER ) + " (specified as " + DelegatingHadoopShim.SUPER_USER
          + " in config.properties)", null );
    }
    sqoopShim = UserSpoofingMaprInvocationHandler.forObject( new CommonSqoopShim() {
      @Override
      public int runTool( String[] args, Configuration c ) {
        hBaseShimInterface.setInfo( ShimUtils.asConfiguration( c ) );
        return super.runTool( args, c );
      }
    }, new HashSet<Class<?>>(), hadoopShim.createConfiguration().get( SQOOP_PROXY_USER ), isRoot );
    shimMap.put( HadoopShim.class, hadoopShim );
    shimMap.put( OozieClientFactory.class, oozieClientFactory );
    shimMap.put( HBaseShimInterface.class, hBaseShimInterface );
    shimMap.put( SqoopShim.class, sqoopShim );
  }

  @Override
  public synchronized <T extends PentahoHadoopShim> T getShim( Class<T> clazz ) {
    @SuppressWarnings( "unchecked" )
    T result = (T) shimMap.get( clazz );
    if ( result == null ) {
      String shimUser = userMap.get( clazz );
      if ( shimUser != null ) {
        shimUser = hadoopShim.createConfiguration().get( shimUser );
      }
      Set<Class<?>> delegateSet = delegateMap.get( clazz );
      if ( delegateSet == null ) {
        delegateSet = new HashSet<Class<?>>();
      }
      result =
          UserSpoofingMaprInvocationHandler.forObject( super.getShim( clazz ), new HashSet<Class<?>>( delegateSet ),
              shimUser, isRoot );
    }
    return result;
  }
}
