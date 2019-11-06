/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.hadoop.shim.pvfs.conf.HCPConf;
import org.pentaho.hadoop.shim.pvfs.conf.PvfsConf;
import org.pentaho.hadoop.shim.pvfs.conf.S3Conf;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PvfsHadoopBridge extends FileSystem {

  private FileSystem fs;

  private final List<PvfsConf.ConfFactory> confFactories;
  private final ConnectionManager connMgr;

  public PvfsHadoopBridge() {
    confFactories = Arrays.asList( S3Conf::new, HCPConf::new );
    connMgr = ConnectionManager.getInstance();
  }

  @VisibleForTesting PvfsHadoopBridge( List<PvfsConf.ConfFactory> confFactories, ConnectionManager connMgr ) {
    this.confFactories = confFactories;
    this.connMgr = connMgr;
  }

  @Override public String getScheme() {
    return "pvfs";
  }

  @Override public Path makeQualified( Path path ) {
    getFs( path );
    return super.makeQualified( updatePath( path ) );
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

  @Override public FileStatus[] listStatus( Path path ) throws IOException {
    return getFs( path ).listStatus( updatePath( path ) );
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
    return getFs( path ).getFileStatus( updatePath( path ) );
  }

  private Path updatePath( Path path ) {
    if ( !getScheme().equals( path.toUri().getScheme() ) ) {
      return path;
    }
    ConnectionDetails details = connMgr.getConnectionDetails( path.toUri().getHost() );
    if ( details == null ) {
      throw new IllegalStateException( "Could not find named connection " + path.toUri().getHost() );
    }
    return getPvfsConf( details ).mapPath( path );
  }


  private FileSystem getFs( Path path ) {
    if ( fs != null ) {
      return fs;
    }
    ConnectionDetails details = connMgr.getConnectionDetails( path.toUri().getHost() );
    PvfsConf confHandler = getPvfsConf( details );

    try {
      fs = FileSystem.get( confHandler.mapPath( path ).toUri(),
        confHandler.conf( path ) );
      this.setConf( confHandler.conf( path ) );
      return fs;
    } catch ( IOException e ) {
      throw new IllegalStateException( e );
    }
  }

  private PvfsConf getPvfsConf( ConnectionDetails details ) {
    return confFactories.stream()
      .map( f -> f.get( details ) )
      .filter( PvfsConf::supportsConnection )
      .findFirst()
      .orElseThrow( () -> new IllegalStateException( "Unsupported VFS connection type:  " + details.getType() ) );
  }

}
