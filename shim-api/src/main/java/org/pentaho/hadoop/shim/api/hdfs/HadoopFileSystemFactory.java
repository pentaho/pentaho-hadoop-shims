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


package org.pentaho.hadoop.shim.api.hdfs;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

import java.io.IOException;
import java.net.URI;

/**
 * Created by bryan on 5/28/15.
 */
public interface HadoopFileSystemFactory {

  boolean canHandle( NamedCluster namedCluster );

  @Deprecated
  HadoopFileSystem create( NamedCluster namedCluster ) throws IOException;

  HadoopFileSystem create( NamedCluster namedCluster, URI uri ) throws IOException;
}
