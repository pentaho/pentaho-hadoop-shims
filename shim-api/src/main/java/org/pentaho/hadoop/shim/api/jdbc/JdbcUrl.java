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


package org.pentaho.hadoop.shim.api.jdbc;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

/**
 * Created by bryan on 4/4/16.
 */
public interface JdbcUrl {
  @Override String toString();

  void setQueryParam( String key, String value );

  String getQueryParam( String key );

  NamedCluster getNamedCluster() throws MetaStoreException;

  String getHost();
}
