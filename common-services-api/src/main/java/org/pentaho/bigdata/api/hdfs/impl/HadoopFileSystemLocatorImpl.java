/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
