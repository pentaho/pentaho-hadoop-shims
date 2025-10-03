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


package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilterFactory;
import org.w3c.dom.Node;

/**
 * Created by bryan on 1/21/16.
 */
public class ColumnFilterFactoryImpl implements ColumnFilterFactory {
  @Override public ColumnFilter createFilter( Node filterNode ) {
    return new ColumnFilterImpl( org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter.getFilter( filterNode ) );
  }

  @Override public ColumnFilter createFilter( Repository rep, int nodeNum, ObjectId id_step ) throws KettleException {
    return new ColumnFilterImpl(
      org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter.getFilter( rep, nodeNum, id_step ) );
  }

  @Override public ColumnFilter createFilter( String alias ) {
    return new ColumnFilterImpl( new org.pentaho.hadoop.shim.api.internal.hbase.ColumnFilter( alias ) );
  }
}
