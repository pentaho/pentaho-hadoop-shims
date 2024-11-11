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


package org.pentaho.bigdata.api.hdfs.impl;

import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemLocatorImpl implements HadoopFileSystemLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemLocatorImpl.class );
  private final List<HadoopFileSystemFactory> hadoopFileSystemFactories;

  public HadoopFileSystemLocatorImpl( List<HadoopFileSystemFactory> hadoopFileSystemFactories ) {
    this.hadoopFileSystemFactories = hadoopFileSystemFactories;
  }

  @Override public HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster )
    throws ClusterInitializationException {
    return getHadoopFilesystem( namedCluster, null );
  }

  @Override
  public HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster, URI uri )
    throws ClusterInitializationException {
    for ( HadoopFileSystemFactory hadoopFileSystemFactory : hadoopFileSystemFactories ) {
      if ( hadoopFileSystemFactory.canHandle( namedCluster ) ) {
        try {
          return hadoopFileSystemFactory.create( namedCluster, uri );
        } catch ( IOException e ) {
          LOGGER
            .warn( "Unable to create " + uri.getScheme() + " service with " + hadoopFileSystemFactory + " for "
              + namedCluster, e );
        }
      }
    }

    return null;
  }
}
