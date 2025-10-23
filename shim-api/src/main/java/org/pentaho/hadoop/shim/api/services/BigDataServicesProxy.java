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

package org.pentaho.hadoop.shim.api.services;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import java.util.Map;

public interface BigDataServicesProxy {
    NamedClusterServiceLocator getNamedClusterServiceLocator();

    HadoopFileSystemLocator getHadoopFileSystemLocator();

    Map<String, String> getShimIdentifier();
}
