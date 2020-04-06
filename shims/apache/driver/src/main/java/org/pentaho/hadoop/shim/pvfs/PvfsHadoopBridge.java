/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.pvfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.ConnectionProvider;
import org.pentaho.di.connections.vfs.provider.ConnectionFileName;
import org.pentaho.di.connections.vfs.provider.ConnectionFileNameParser;
import org.pentaho.hadoop.shim.api.format.org.pentaho.hadoop.shim.pvfs.api.PvfsHadoopBridgeFileSystemExtension;
import org.pentaho.hadoop.shim.pvfs.conf.HCPConf;
import org.pentaho.hadoop.shim.pvfs.conf.PvfsConf;
import org.pentaho.hadoop.shim.pvfs.conf.S3Conf;
import org.pentaho.hadoop.shim.pvfs.conf.SnwConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.vfs2.FileSystemException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PvfsHadoopBridge extends FileSystem implements PvfsHadoopBridgeFileSystemExtension {

  private FileSystem fs;

  private final List<PvfsConf.ConfFactory> confFactories;
  private final ConnectionManager connMgr;
  private static final Logger LOGGER = LoggerFactory.getLogger( PvfsHadoopBridge.class );

  @SuppressWarnings( "UnstableApiUsage" )
  // Cache was beta in version 11, which is the version hadoop 3.1 uses.
  // the Cache api we use is unchanged with guava 19+, no longer beta.
  private final Cache<PvfsConf, FileSystem> fsCache = CacheBuilder.newBuilder()
    .expireAfterAccess( 1, TimeUnit.HOURS )
    .build();

  @SuppressWarnings( "unused" )
  public PvfsHadoopBridge() {
    confFactories = Arrays.asList( S3Conf::new, HCPConf::new, SnwConf::new );
    connMgr = ConnectionManager.getInstance();
  }

  @VisibleForTesting PvfsHadoopBridge( List<PvfsConf.ConfFactory> confFactories, ConnectionManager connMgr ) {
    this.confFactories = confFactories;
    this.connMgr = connMgr;
  }

  @Override public String getScheme() {
    return "pvfs";
  }

  @Override protected void checkPath( Path path ) {
    if ( getFs( path ) == null ) {
      throw new IllegalArgumentException( "Cannot find a supported filesystem for " + path );
    }
  }

  @Override public Path makeQualified( Path path ) {
    getFs( path );
    return super.makeQualified( path );
  }

  @Override public URI getUri() {
    Objects.requireNonNull( fs );
    return fs.getUri();
  }

  @Override public FSDataInputStream open( Path path, int i ) throws IOException {
    return getFs( path ).open( updatePath( path ), i );
  }

  @Override public FSDataOutputStream create( Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                              Progressable progressable ) throws IOException {
    return getFs( path ).create( updatePath( path ), fsPermission, b, i, i1, l, progressable );
  }

  @Override public FSDataOutputStream append( Path path, int i, Progressable progressable ) throws IOException {
    return getFs( path ).append( updatePath( path ), i, progressable );
  }

  @Override public boolean rename( Path path, Path path1 ) throws IOException {
    return getFs( path ).rename( updatePath( path ), updatePath( path1 ) );
  }

  @Override public boolean delete( Path path, boolean b ) throws IOException {
    return getFs( path ).delete( updatePath( path ), b );
  }

  @Override public void setWorkingDirectory( Path path ) {
    getFs( path ).setWorkingDirectory( updatePath( path ) );
  }

  @Override public Path getWorkingDirectory() {
    Objects.requireNonNull( fs );
    return fs.getWorkingDirectory();
  }

  @Override public boolean mkdirs( Path path, FsPermission fsPermission ) throws IOException {
    return getFs( path ).mkdirs( updatePath( path ), fsPermission );
  }

  @Override public FileStatus getFileStatus( Path path ) throws IOException {
    FileStatus fileStatus = getFs( path ).getFileStatus( updatePath( path ) );
    fileStatus.setPath( path );
    return fileStatus;
  }

  @Override public FileStatus[] listStatus( Path path ) throws IOException {
    FileStatus[] fileStatuses = getFs( path ).listStatus( updatePath( path ) );
    Arrays.stream( fileStatuses )
      .forEach( status -> status.setPath(
        getPvfsConf( path ).mapPath( path, status.getPath() ) ) );
    return fileStatuses;
  }

  private Path updatePath( Path path ) {
    if ( schemeIsNotPvfs( path ) ) {
      return path;
    }

    Path updatedPath = getPvfsConf( path ).mapPath( path );

    return new Path( updatedPath.toUri() ) {
      @Override public FileSystem getFileSystem( Configuration conf ) {
        return getCachedFs( path );
      }
    };
  }

  private boolean schemeIsNotPvfs( Path path ) {
    return !getScheme().equals( path.toUri().getScheme() );
  }


  private FileSystem getFs( Path path ) {
    if ( schemeIsNotPvfs( path ) ) {
      // if path does not have a pvfs schema than we assume it's the scheme of the underlying
      // filesystem.  It's required that fs has already been initialized, since we need
      // connection details to map to the correct filesystem.
      return Objects.requireNonNull( fs, "File system not initialized for " + path.toString() );
    }
    return getCachedFs( path );
  }

  /**
   * Retrieve the fs from the local cache, read-through if not present.
   * <p>
   * We use our own cache because the {@link org.apache.hadoop.fs.FileSystem} cache only uses the Schema, Authority, and
   * UGI for key definition.  This can cause inconsistencies in PDI if 1)  The connection details have been modified. 2)
   * More than one connection details definition uses the same Scheme, Authority, and UGI.
   * <p>
   * `Authority` in the scope of PVFS can mean the bucket in S3, for example, or the namespace for HCP.
   * <p>
   * PvfsConf implementations should disable the FileSystem cache with
   * <p>
   * fs.s3a.impl.disable.cache=true
   * <p>
   * where `s3a` is appropriate to the underlying filesystem.
   */
  private FileSystem getCachedFs( Path path ) {
    PvfsConf pvfsConf = getPvfsConf( path );
    try {
      fs = fsCache.get( pvfsConf, () -> getRealFileSystem( path, pvfsConf ) );
      return fs;
    } catch ( ExecutionException e ) {
      throw new IllegalStateException( e );
    }
  }

  private FileSystem getRealFileSystem( Path path, PvfsConf pvfsConf ) {
    try {
      Configuration conf = pvfsConf.conf( path );
      fs = FileSystem.get( pvfsConf.mapPath( path ).toUri(), conf );
      this.setConf( conf );
      return fs;
    } catch ( IOException e ) {
      throw new IllegalStateException( e );
    }
  }

  private PvfsConf getPvfsConf( Path path ) {
    ConnectionDetails details = getConnectionDetails( path );
    if ( details == null ) {
      throw new IllegalStateException( "Could not find named connection " + path.toUri().getHost() );
    }
    return confFactories.stream()
      .map( f -> f.get( details ) )
      .filter( PvfsConf::supportsConnection )
      .findFirst()
      .orElseThrow(
        () -> new IllegalStateException( "Unsupported VFS connection type:  " + getProviderName( details ) ) );
  }

  @VisibleForTesting ConnectionDetails getConnectionDetails( Path path ) {
    return connMgr.getConnectionDetails( getConnectionName( path ) );
  }

  /**
   * Retrieves the Pentaho VFS connection name associated with path, if one is present.
   *
   * @param path input path, expected to have pvfs scheme.
   * @return PVFS connection name
   */
  public static String getConnectionName( Path path ) {
    try {
      return ( (ConnectionFileName) new ConnectionFileNameParser()
        .parseUri( null, null, path.toString() ) ).getConnection();
    } catch ( FileSystemException e ) {
      LOGGER.warn( "Failed to retrieve connection details with unexpected exception", e );
      return null;
    }
  }

  private String getProviderName( ConnectionDetails details ) {
    return connMgr.getProviders().stream()
      .filter( connectionProvider -> connectionProvider.getKey().equals( details.getType() ) )
      .findFirst()
      .map( ConnectionProvider::getName )
      .orElse( details.getType() );
  }

  public String generateAlias( String pvfsPath ) {
    Path path = new Path( pvfsPath );
    return getPvfsConf( path ).generateAlias( pvfsPath );
  }
}
