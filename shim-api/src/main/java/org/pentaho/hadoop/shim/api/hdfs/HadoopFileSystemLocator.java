/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.hdfs;

import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;

import java.net.URI;

/**
 * Created by bryan on 5/22/15.
 */
public interface HadoopFileSystemLocator {
  @Deprecated
  HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster ) throws ClusterInitializationException;

  HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster, URI uri ) throws ClusterInitializationException;
}
