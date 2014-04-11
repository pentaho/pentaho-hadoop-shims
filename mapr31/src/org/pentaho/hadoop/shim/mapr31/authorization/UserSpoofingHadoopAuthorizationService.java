package org.pentaho.hadoop.shim.mapr31.authorization;

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
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.DistributedCacheUtil;
import org.pentaho.hadoop.shim.mapr31.MapR3DistributedCacheUtilImpl;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
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
  protected static final String PMR_STAGE_PROXY_USER = "pentaho.pmr.staging.proxy.user";

  private final Map<Class<?>, String> userMap;
  private final Map<Class<?>, Set<Class<?>>> delegateMap;
  private final Map<Class<?>, PentahoHadoopShim> shimMap;
  private final UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable;
  private final HadoopShim hadoopShim;
  private final OozieClientFactory oozieClientFactory;
  private boolean isRoot;

  public UserSpoofingHadoopAuthorizationService(
      final UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable )
    throws AuthenticationConsumptionException {
    this.userSpoofingHadoopAuthorizationCallable = userSpoofingHadoopAuthorizationCallable;
    isRoot = this.userSpoofingHadoopAuthorizationCallable.call().getUserCreds().getIsRoot();
    userMap = new HashMap<Class<?>, String>();
    userMap.put( PigShim.class, PIG_PROXY_USER );
    userMap.put( SqoopShim.class, SQOOP_PROXY_USER );
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
        return createConfiguration().get( PMR_STAGE_PROXY_USER );
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
    }, new HashSet<Class<?>>( Arrays.<Class<?>> asList( DistributedCacheUtil.class ) ) );
    oozieClientFactory =
        KerberosInvocationHandler.forObject( userSpoofingHadoopAuthorizationCallable.getLoginContext(),
            new OozieClientFactoryImpl( hadoopShim.createConfiguration().get( OOZIE_PROXY_USER ) ),
            new HashSet<Class<?>>( Arrays.<Class<?>> asList( OozieClientFactory.class, OozieClient.class,
                OozieJob.class ) ) );
    shimMap.put( HadoopShim.class, hadoopShim );
    shimMap.put( OozieClientFactory.class, oozieClientFactory );
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
