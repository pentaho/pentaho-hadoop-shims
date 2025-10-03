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


package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemFactoryImpl implements HadoopFileSystemFactory {
  public static final String SHIM_IDENTIFIER = "shim.identifier";
  public static final String HDFS = "hdfs";
  protected static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemFactoryImpl.class );
  protected final boolean isActiveConfiguration;
  protected final HadoopShim hadoopShim;
  protected final ShimIdentifierInterface shimIdentifier;

  public HadoopFileSystemFactoryImpl( HadoopShim hadoopShim, ShimIdentifierInterface shimIdentifier ) {
    this( true, hadoopShim, "hdfs", shimIdentifier );
  }

  public HadoopFileSystemFactoryImpl( boolean isActiveConfiguration, HadoopShim hadoopShim,
                                      String scheme, ShimIdentifierInterface shimIdentifier ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopShim = hadoopShim;
    this.shimIdentifier = shimIdentifier;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = namedCluster.getShimIdentifier();
    //handle only if we do not use gateway
    return ( shimIdentifier == null && !namedCluster.isUseGateway() )
      || ( this.shimIdentifier.getId().equals( shimIdentifier ) && !namedCluster.isUseGateway() );
  }
  @Override
  public HadoopFileSystem create( NamedCluster namedCluster ) throws IOException {
    return create( namedCluster, null );
  }

  @Override
  public HadoopFileSystem create( NamedCluster namedCluster, URI uri ) throws IOException {
    final Configuration configuration = hadoopShim.createConfiguration( namedCluster );
    FileSystem fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
    if ( fileSystem instanceof LocalFileSystem ) {
      LOGGER.error( "Got a local filesystem, was expecting an hdfs connection" );
      throw new IOException( "Got a local filesystem, was expecting an hdfs connection" );
    }

    final URI finalUri = fileSystem.getUri() != null ? fileSystem.getUri() : uri;
    HadoopFileSystem hadoopFileSystem = new HadoopFileSystemImpl( () -> {
      try {
        return finalUri != null
          ? (FileSystem) hadoopShim.getFileSystem( finalUri, configuration, (NamedCluster) namedCluster ).getDelegate()
          : (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
      } catch ( IOException | InterruptedException e ) {
        LOGGER.debug( "Error looking up/creating the file system ", e );
        return null;
      }
    } );
    ( (HadoopFileSystemImpl) hadoopFileSystem ).setNamedCluster( namedCluster );

    return hadoopFileSystem;
  }
}
