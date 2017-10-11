/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common.fs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.common.ShimUtils;

public class FileSystemProxy extends org.apache.hadoop.fs.FileSystem implements FileSystem {

  private org.apache.hadoop.fs.FileSystem delegate;

  public FileSystemProxy( org.apache.hadoop.fs.FileSystem delegateParam ) {
    if ( delegateParam == null ) {
      throw new NullPointerException();
    }
    this.delegate = delegateParam;
  }

  @Override
  public Object getDelegate() {
    return delegate;
  }

  protected org.apache.hadoop.fs.FileSystem getDelegate( org.apache.hadoop.fs.Path hadoopPath ) {
    return (org.apache.hadoop.fs.FileSystem) getDelegate();
  }

  protected org.apache.hadoop.fs.FileSystem getDelegate( org.pentaho.hadoop.shim.api.fs.Path pentahoPath ) {
    return getDelegate( ShimUtils.asPath( pentahoPath ) );
  }

  @Override
  public org.pentaho.hadoop.shim.api.fs.Path asPath( String path ) {
    return new PathProxy( path );
  }

  @Override
  public org.pentaho.hadoop.shim.api.fs.Path asPath( org.pentaho.hadoop.shim.api.fs.Path parent, String child ) {
    return new PathProxy( parent, child );
  }

  @Override
  public org.pentaho.hadoop.shim.api.fs.Path asPath( String parent, String child ) {
    return new PathProxy( parent, child );
  }

  @Override
  public boolean exists( org.pentaho.hadoop.shim.api.fs.Path path ) throws IOException {
    return getDelegate( path ).exists( ShimUtils.asPath( path ) );
  }

  @Override
  public boolean delete( org.pentaho.hadoop.shim.api.fs.Path path, boolean recursive ) throws IOException {
    return delete( ShimUtils.asPath( path ), recursive );
  }

  // DELEGATING METHODS  
  @Override
  public FSDataOutputStream append( Path f, int bufferSize, Progressable progress ) throws IOException {
    return getDelegate( f ).append( f, bufferSize, progress );
  }

  @Override
  public FSDataOutputStream create( Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                    short replication, long blockSize, Progressable progress ) throws IOException {
    return getDelegate( f ).create( f, overwrite, bufferSize, replication, blockSize, progress );
  }

  @Override
  @Deprecated
  public boolean delete( Path f ) throws IOException {
    return getDelegate( f ).delete( f );
  }

  @Override
  public boolean delete( Path f, boolean recursive ) throws IOException {
    return getDelegate( f ).delete( f, recursive );
  }

  @Override
  public FileStatus getFileStatus( Path f ) throws IOException {
    return getDelegate( f ).getFileStatus( f );
  }

  @Override
  public URI getUri() {
    return delegate.getUri();
  }

  @Override
  public Path getWorkingDirectory() {
    return delegate.getWorkingDirectory();
  }

  @Override
  public FileStatus[] listStatus( Path f ) throws IOException {
    return getDelegate( f ).listStatus( f );
  }

  @Override
  public boolean mkdirs( Path f, FsPermission permission ) throws IOException {
    return getDelegate( f ).mkdirs( f, permission );
  }

  @Override
  public FSDataInputStream open( Path f, int bufferSize ) throws IOException {
    return getDelegate( f ).open( f, bufferSize );
  }

  @Override
  public boolean rename( Path src, Path dst ) throws IOException {
    return getDelegate( src ).rename( src, dst );
  }

  @Override
  public void setWorkingDirectory( Path f ) {
    getDelegate( f ).setWorkingDirectory( f );
  }
}
