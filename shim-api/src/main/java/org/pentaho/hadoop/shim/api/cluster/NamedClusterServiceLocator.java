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

package org.pentaho.hadoop.shim.api.cluster;


import java.util.List;

/**
 * Created by bryan on 11/5/15.
 */
public interface NamedClusterServiceLocator {
  <T> T getService( NamedCluster namedCluster, Class<T> serviceClass ) throws ClusterInitializationException;

  <T> T getService( NamedCluster namedCluster, Class<T> serviceClass, String embeddedMetaStoreProviderKey )
    throws ClusterInitializationException;

  List<String> getVendorShimList();
}
