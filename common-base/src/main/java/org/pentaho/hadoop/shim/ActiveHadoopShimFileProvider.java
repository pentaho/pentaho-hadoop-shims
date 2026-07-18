/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.hadoop.shim;

import java.util.Collection;
import java.util.Collections;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.FileProvider;

/**
 * Proxies the active {@link HadoopConfiguration}'s {@link FileProvider}. This is used to be able to swap out the Hadoop
 * configuration at runtime while registering multiple file providers under the same scheme.
 */
public class ActiveHadoopShimFileProvider implements FileProvider {

  private HadoopConfigurationFileSystemManager fsm;
  private String scheme;

  public ActiveHadoopShimFileProvider( HadoopConfigurationFileSystemManager fsm, String scheme ) {
    if ( fsm == null || scheme == null ) {
      throw new NullPointerException();
    }
    this.fsm = fsm;
    this.scheme = scheme;
  }

  @Override
  public FileObject createFileSystem( String scheme, FileObject file, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    FileProvider p = fsm.getActiveFileProvider( scheme );
    Thread.currentThread().setContextClassLoader( p.getClass().getClassLoader() );
    try {
      return p.createFileSystem( scheme, file, fileSystemOptions );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public FileObject findFile( FileObject baseFile, String uri, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    FileProvider p = fsm.getActiveFileProvider( scheme );
    Thread.currentThread().setContextClassLoader( p.getClass().getClassLoader() );
    try {
      return p.findFile( baseFile, uri, fileSystemOptions );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public Collection<Capability> getCapabilities() {
    try {
      return fsm.getActiveFileProvider( scheme ).getCapabilities();
    } catch ( FileSystemException e ) {
      return Collections.emptyList();
    }
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    try {
      return fsm.getActiveFileProvider( scheme ).getConfigBuilder();
    } catch ( FileSystemException e ) {
      return null;
    }
  }

  @Override
  public FileName parseUri( FileName root, String uri ) throws FileSystemException {
    return fsm.getActiveFileProvider( scheme ).parseUri( root, uri );
  }

}