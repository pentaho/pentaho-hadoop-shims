package org.pentaho.hadoop.shim.mapr31.authorization;

import java.util.HashSet;
import java.util.concurrent.Callable;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.mapr31.MapR3DistributedCacheUtilImpl;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hdfs.vfs.HadoopFileSystem;
import org.pentaho.hdfs.vfs.MapRFileProvider;
import org.pentaho.hdfs.vfs.MapRFileSystem;

public class UserSpoofingHadoopAuthorizationService extends NoOpHadoopAuthorizationService implements
    HadoopAuthorizationService {
  protected static final String HDFS_PROXY_USER = "pentaho.hdfs.proxy.user";
  protected static final String MR_PROXY_USER = "pentaho.mapreduce.proxy.user";
  private final UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable;
  private boolean isRoot;

  public UserSpoofingHadoopAuthorizationService(
      UserSpoofingHadoopAuthorizationCallable userSpoofingHadoopAuthorizationCallable )
    throws AuthenticationConsumptionException {
    this.userSpoofingHadoopAuthorizationCallable = userSpoofingHadoopAuthorizationCallable;
    isRoot = this.userSpoofingHadoopAuthorizationCallable.call().getUserCreds().getIsRoot();
  }

  @Override
  public HadoopShim getHadoopShim() {
    return UserSpoofingInvocationHandler.forObject( new org.pentaho.hadoop.shim.mapr31.HadoopShim() {

      public String getFileSystemGetUser( Configuration conf ) {
        return conf.get( HDFS_PROXY_USER );
      }

      @SuppressWarnings( "unused" )
      public String submitJobGetUser( Configuration conf ) {
        return conf.get( MR_PROXY_USER );
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
                  return UserSpoofingInvocationHandler.forObject( callable, new HashSet<Class<?>>(),
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
    }, new HashSet<Class<?>>() );
  }
}
